import boto3
import botocore
# import jsonschema
import json
import traceback
import fastjsonschema

from extutil import remove_none_attributes, account_context, ExtensionHandler, ext, \
    current_epoch_time_usec_num, component_safe_name, handle_common_errors

from botocore.exceptions import ClientError

eh = ExtensionHandler()

s3 = boto3.client("s3")

def lambda_handler(event, context):
    try:
        print(f"event = {event}")
        # account_number = account_context(context)['number']
        # region = account_context(context)['region']
        eh.capture_event(event)
        # prev_state = event.get("prev_state")
        cdef = event.get("component_def")
        project_code = event.get("project_code")
        repo_id = event.get("repo_id")
        cname = event.get("component_name")
        bucket_name = eh.props.get("name") or cdef.get("name") or component_safe_name(project_code, repo_id, cname, no_underscores=True, no_uppercase=True, max_chars=63)
        print(f"bucket_name = {bucket_name}")

        pass_back_data = event.get("pass_back_data", {})
        if pass_back_data:
            pass
        elif event.get("op") == "upsert":
            if cdef.get("notification_configuration"):
                eh.add_op("put_bucket_notification_configuration")
            else:
                eh.add_op("delete_bucket_notification_configuration")
        elif event.get("op") == "delete":
            eh.add_op("delete_bucket_notification_configuration")

        put_bucket_notification_configuration(bucket_name, cdef)
        delete_bucket_notification_configuration(bucket_name)

        return eh.finish()

    except Exception as e:
        msg = traceback.format_exc()
        print(msg)
        eh.add_log("Unexpected Error", {"error": msg}, is_error=True)
        eh.declare_return(200, 0, error_code=str(e))
        return eh.finish()

@ext(handler=eh, op="put_bucket_notification_configuration")
def put_bucket_notification_configuration(bucket_name, cdef):
    notification_config = cdef.get("notification_configuration")
    # Looks like the following:
    # [
    #    {
    #        "arn": <either a function arn, a queue arn, or a topic arn>,
    #        "events": [<list of events>],
    #        "filter_prefixes": [<list of prefixes>],
    #        "filter_suffixes": [<list of suffixes>]
    #    }
    # ]

    print(f"notification_config = {notification_config}")

    validate_notification_config(notification_config)

    not_config = {}
    for config in notification_config:
        if config.get("arn").startswith("arn:aws:lambda"):
            if not not_config.get("LambdaFunctionConfigurations"):
                not_config["LambdaFunctionConfigurations"] = []
            not_config["LambdaFunctionConfigurations"].append(remove_none_attributes({
                "LambdaFunctionArn": config.get("arn"),
                "Events": config.get("events"),
                "Filter": remove_none_attributes({
                    "Key": remove_none_attributes({
                        "FilterRules": ([
                            remove_none_attributes({
                                "Name": "prefix",
                                "Value": prefix
                            }) for prefix in config.get("filter_prefixes", [])
                        ] + [
                            remove_none_attributes({
                                "Name": "suffix",
                                "Value": suffix
                            }) for suffix in config.get("filter_suffixes", [])
                        ]) or None
                    }) or None
                }) or None
            }))
        
        elif config.get("arn").startswith("arn:aws:sqs"):
            if not "QueueConfigurations" in not_config:
                not_config["QueueConfigurations"] = []
            not_config["QueueConfigurations"].append(remove_none_attributes({
                "QueueArn": config.get("arn"),
                "Events": config.get("events"),
                "Filter": remove_none_attributes({
                    "Key": remove_none_attributes({
                        "FilterRules": ([
                            remove_none_attributes({
                                "Name": "prefix",
                                "Value": prefix
                            }) for prefix in config.get("filter_prefixes", [])
                        ] + [
                            remove_none_attributes({
                                "Name": "suffix",
                                "Value": suffix
                            }) for suffix in config.get("filter_suffixes", [])
                        ]) or None
                    }) or None
                }) or None
            }))

        elif config.get("arn").startswith("arn:aws:sns"):
            if not not_config.get("TopicConfigurations"):
                not_config["TopicConfigurations"] = []
            not_config["TopicConfigurations"].append(remove_none_attributes({
                "TopicArn": config.get("arn"),
                "Events": config.get("events"),
                "Filter": remove_none_attributes({
                    "Key": remove_none_attributes({
                        "FilterRules": ([
                            remove_none_attributes({
                                "Name": "prefix",
                                "Value": prefix
                            }) for prefix in config.get("filter_prefixes", [])
                        ] + [
                            remove_none_attributes({
                                "Name": "suffix",
                                "Value": suffix
                            }) for suffix in config.get("filter_suffixes")
                        ]) or None
                    }) or None
                }) or None
            }))


    params = {
        "Bucket": bucket_name,
        "NotificationConfiguration": not_config,
        "SkipDestinationValidation": True
    }

    print(f"params = {params}")

    try:
        _ = s3.put_bucket_notification_configuration(**params)
        eh.add_log("Notification Config Set", {"config": notification_config})

    except ClientError as e:
        handle_common_errors(e, eh, "Put Bucket Notification Config Error", 20)

@ext(handler=eh, op="delete_bucket_notification_configuration")
def delete_bucket_notification_configuration(bucket_name):
    try:
        _ = s3.put_bucket_notification_configuration(Bucket=bucket_name, NotificationConfiguration={})
        eh.add_log("Notification Config Deleted", {"bucket_name": bucket_name})

    except ClientError as e:
        handle_common_errors(e, eh, "Delete Bucket Notification Config Error", 20)

def validate_notification_config(config):
    schema = {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "arn": {
                    "type": "string",
                    "description": "The ARN of the lambda function, SNS topic, or SQS queue to send notifications to"
                },
                "events": {
                    "type": "array",
                    "description": "The events to send notifications for",
                    "minItems": 1,
                    "uniqueItems": True,
                    "items": {
                        "type": "string",
                        "enum": [
                            's3:ReducedRedundancyLostObject',
                            's3:ObjectCreated:*','s3:ObjectCreated:Put',
                            's3:ObjectCreated:Post','s3:ObjectCreated:Copy',
                            's3:ObjectCreated:CompleteMultipartUpload',
                            's3:ObjectRemoved:*','s3:ObjectRemoved:Delete',
                            's3:ObjectRemoved:DeleteMarkerCreated',
                            's3:ObjectRestore:*','s3:ObjectRestore:Post',
                            's3:ObjectRestore:Completed','s3:Replication:*',
                            's3:Replication:OperationFailedReplication',
                            's3:Replication:OperationNotTracked',
                            's3:Replication:OperationMissedThreshold',
                            's3:Replication:OperationReplicatedAfterThreshold',
                            's3:ObjectRestore:Delete','s3:LifecycleTransition',
                            's3:IntelligentTiering','s3:ObjectAcl:Put',
                            's3:LifecycleExpiration:*',
                            's3:LifecycleExpiration:Delete',
                            's3:LifecycleExpiration:DeleteMarkerCreated',
                            's3:ObjectTagging:*',
                            's3:ObjectTagging:Put','s3:ObjectTagging:Delete',
                        ],
                    }
                },
                "filter_prefixes": {
                    "type": "array",
                    "description": "The prefixes to filter notifications for",
                    "items": {
                        "type": "string"
                    }
                },
                "filter_suffixes": {
                    "type": "array",
                    "description": "The suffixes to filter notifications for",
                    "items": {
                        "type": "string"
                    }
                }
            },
            "required": ["arn", "events"]
        }
    }
    fastjsonschema.validate(schema, config)
    

