import boto3
import botocore
# import jsonschema
import json
import traceback
import fastjsonschema

from extutil import remove_none_attributes, account_context, ExtensionHandler, ext, \
    current_epoch_time_usec_num, component_safe_name, handle_common_errors, \
    random_id

from botocore.exceptions import ClientError

eh = ExtensionHandler()

s3 = boto3.client('s3')

def lambda_handler(event, context):
    try:
        print(f"event = {event}")
        # account_number = account_context(context)['number']
        region = account_context(context)['region']
        eh.capture_event(event)
        prev_state = event.get("prev_state") or {}
        cdef = event.get("component_def")
        project_code = event.get("project_code")
        repo_id = event.get("repo_id")
        cname = event.get("component_name")
        bucket_name = prev_state.get("props", {}).get("name") or cdef.get("name") or component_safe_name(project_code, repo_id, cname, no_underscores=True, no_uppercase=True, max_chars=63)
        allow_alternate_bucket_name = cdef.get("allow_alternate_bucket_name", True)
        print(f"bucket_name = {bucket_name}")

        # Supporting alternate bucket names
        if not eh.state.get("bucket_name"):
            eh.state["bucket_name"] = bucket_name

        pass_back_data = event.get("pass_back_data", {})
        if pass_back_data:
            pass
        elif event.get("op") == "upsert":
            eh.add_op("get_bucket")
        elif event.get("op") == "delete":
            eh.add_op("delete_bucket")

        get_bucket(cdef, region, prev_state)
        create_bucket(cdef, region, allow_alternate_bucket_name)
        get_public_access_block(cdef)
        set_public_access_block(cdef)
        delete_public_access_block()
        get_bucket_policy(cdef)
        set_bucket_policy(cdef)
        delete_bucket_policy()
        set_versioning(cdef)
        get_cors(cdef)
        set_cors(cdef)
        delete_cors()
        set_tags()
        remove_all_tags()
        put_bucket_website(cdef)
        delete_bucket_website()
        get_bucket_encryption(cdef)
        set_bucket_encryption()
        delete_bucket()

        return eh.finish()

    except Exception as e:
        msg = traceback.format_exc()
        print(msg)
        eh.add_log("Unexpected Error", {"error": msg}, is_error=True)
        eh.declare_return(200, 0, error_code=str(e))
        return eh.finish()

def format_tags(tags_dict):
    return [{"Key": k, "Value": v} for k,v in tags_dict.items()]

@ext(handler=eh, op="get_bucket")
def get_bucket(cdef, region, prev_state):
    bucket_name = eh.state.get("bucket_name")

    if prev_state and prev_state.get("props") and prev_state.get("props").get("name"):
        prev_bucket_name = prev_state.get("props").get("name")
        if bucket_name != prev_bucket_name:
            eh.perm_error("Cannot Change Bucket Name", progress=0)
            return None
    
    try:
        s3_resource = boto3.resource("s3")
        response = s3_resource.meta.client.head_bucket(Bucket=bucket_name)
        if response:
            print(response)
            eh.add_props({"region": response['ResponseMetadata']['HTTPHeaders']['x-amz-bucket-region']})
    except ClientError as e:
        if int(e.response['Error']['Code']) == 404:
            eh.add_log("Bucket Does Not Exist", {"name": bucket_name})
            eh.add_op("create_bucket")
            return 0
        elif int(e.response['Error']['Code']) == 403:
            eh.add_log("Bucket Owned By Other Account", {"name": bucket_name})
            if cdef.get("allow_alternate_bucket_name"):
                eh.state['bucket_name'] = generate_alternate_bucket_name()
                eh.add_op("create_bucket")
            else:
                eh.perm_error("Bucket Owned By Another Account", 0)
            return 0
        else:
            print(str(e))
            eh.add_log("Get Bucket Error", {"error": str(e)}, is_error=True)
            eh.retry_error("Get Bucket Error", 30)
            return 0

    try:
        response = s3.get_bucket_versioning(Bucket=bucket_name)
        eh.add_log("Got Bucket Versioning", response)
        versioning = not ((not response.get("Status")) or (response.get("Status") == "Suspended"))
        want_versioning = bool(cdef.get("versioning"))
        if versioning != want_versioning:
            eh.add_op("set_versioning", want_versioning)

        eh.add_props({
            "name": bucket_name,
            "arn":gen_bucket_arn(bucket_name),
            "all_objects_arn": gen_bucket_arn(bucket_name) + "/*"
        })

        if prev_state and prev_state.get("props") and prev_state.get("props").get("region"):
            eh.add_props({
                "region": prev_state.get("props").get("region")
            })

        eh.add_links({"Bucket": gen_bucket_link(bucket_name)})
        eh.add_op("set_bucket_acl")
        eh.add_op("get_public_access_block")
        eh.add_op("get_bucket_policy")
        eh.add_op("get_bucket_cors")

        if cdef.get("tags"):
            eh.add_op("set_tags", cdef['tags'])
        else:
            eh.add_op("remove_all_tags")

        if cdef.get("website_configuration"):
            eh.add_op("put_bucket_website")
        else:
            eh.add_op("delete_bucket_website")

        eh.add_op("get_bucket_encryption")

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "NoSuchBucket":
            eh.add_log("Bucket Does Not Exist", {"bucket_name": bucket_name})
            eh.add_op("create_bucket")
            return None
        else:
            print(str(e))
            eh.retry_error("Get Bucket Versioning Error", 30)
            eh.add_log("Get Bucket Versioning Error", {"error": str(e)}, is_error=True)
            return None
            

@ext(handler=eh, op="create_bucket")
def create_bucket(cdef, region, allow_alternate_bucket_name):
    bucket_name = eh.state.get("bucket_name")

    print(f"region = {region}")
    params = remove_none_attributes({
        # "ACL": cdef.get("ACL"),
        "Bucket": bucket_name,
        "CreateBucketConfiguration": {
            "LocationConstraint": region
        } if not region == "us-east-1" else None
    })

    try:
        location_response = s3.create_bucket(**params)
        eh.add_log("Bucket Created", location_response)
        if cdef.get("public_read") or cdef.get("acl"):
            eh.add_op("set_bucket_acl")

        if ("block_public_access" in cdef) or ("public_access_block" in cdef):
            eh.add_op("set_public_access_block")

        if cdef.get("bucket_policy"):
            eh.add_op("set_bucket_policy")

        if cdef.get("versioning"):
            eh.add_op("set_versioning")

        if cdef.get("CORS"):
            eh.add_op("set_cors")

        if cdef.get("tags"):
            eh.add_op("set_tags", cdef['tags'])

        if cdef.get("website_configuration"):
            eh.add_op("put_bucket_website")

        eh.add_op("get_bucket_encryption")

        eh.add_props({
            "name": bucket_name,
            "arn": gen_bucket_arn(bucket_name),
            "all_objects_arn": gen_bucket_arn(bucket_name) + "/*",
            "region": region
        })

        eh.add_links({"Bucket": gen_bucket_link(bucket_name)})

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "BucketAlreadyExists":
            if allow_alternate_bucket_name:
                eh.add_log("Bucket Owned By Other Account, Switching", {"bucket_name": bucket_name})
                eh.state['bucket_name'] = generate_alternate_bucket_name()
                create_bucket(cdef, region, allow_alternate_bucket_name)
            else:
                eh.perm_error(f"Bucket {bucket_name} Already Exists", 0)
                eh.add_log("Bucket Owned By Other Account, Exiting", {"error": str(e)}, is_error=True)
        elif e.response['Error']['Code'] == "BucketAlreadyOwnedByYou":
            eh.add_log("Bucket Already in this Account", {"error": str(e)})
        else:
            print(str(e))
            eh.retry_error("Create Error", 0)
            eh.add_log("Create Error", {"error": str(e)}, is_error=True)

@ext(handler=eh, op="get_bucket_acl")
def get_bucket_acl(cdef):
    bucket_name = eh.state.get("bucket_name")
    try:
        response = s3.get_bucket_acl(Bucket=bucket_name)
        eh.add_log("Got Bucket ACL", response)
        current_grants = list(filter(lambda grant: grant.get("ID") != response['Owner']['ID'], response.get("Grants")))
        desired_params = get_acl_params_from_cdef(cdef)
        desired_grants = desired_params.get("AccessControlPolicy", {}).get("Grants", [])
        if desired_params.get("GrantFullControl"):
            desired_grants.append({
                "Grantee": get_grantee_from_grant_string(desired_params.get("GrantFullControl")),
                "Permission": "FULL_CONTROL"
            })
        if desired_params.get("GrantRead"):
            desired_grants.append({
                "Grantee": get_grantee_from_grant_string(desired_params.get("GrantRead")),
                "Permission": "READ_ACP"
            })
        if desired_params.get("GrantReadACP"):
            desired_grants.append({
                "Grantee": get_grantee_from_grant_string(desired_params.get("GrantReadACP")),
                "Permission": "READ_ACP"
            })
        if desired_params.get("GrantWrite"):
            desired_grants.append({
                "Grantee": get_grantee_from_grant_string(desired_params.get("GrantWrite")),
                "Permission": "WRITE_ACP"
            })
        if desired_params.get("GrantWriteACP"):
            desired_grants.append({
                "Grantee": get_grantee_from_grant_string(desired_params.get("GrantWriteACP")),
                "Permission": "WRITE_ACP"
            })

        for grant in desired_grants:
            matching_grant = list(filter(lambda x: x["Grantee"].get("ID") == grant["Grantee"].get("ID") and x["Grantee"].get("URI") == grant["Grantee"].get("URI") and x["Permission"] == grant["Permission"], current_grants))
            if len(matching_grant) == 0:
                eh.add_op("set_bucket_acl")
                break

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "NoSuchBucket":
            eh.add_log("Bucket Does Not Exist", {"bucket_name": bucket_name})
        else:
            handle_common_errors(e, eh, "Get Bucket ACL Error", 5)

@ext(handler=eh, op="set_bucket_acl")
def set_bucket_acl(cdef):
    bucket_name = eh.state.get("bucket_name")

    params = get_acl_params_from_cdef(cdef)
    params['Bucket'] = bucket_name
    try:
        _ = s3.put_bucket_acl(**params)
        eh.add_log("Managed ACL", params)

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "BucketAlreadyExists":
            eh.perm_error(f"Bucket {bucket_name} Already Exists", 0)
            eh.add_log("Bucket Already Exists, Exiting", {"error": str(e)}, is_error=True)
            return None
        elif e.response['Error']['Code'] == "BucketAlreadyOwnedByYou":
            eh.add_log("Bucket Already in this Account", {"error": str(e)})
            return None
        else:
            print(str(e))
            eh.retry_error("Public ACL Error", 7)
            eh.add_log("Put ACL Error", {"error": str(e)}, is_error=True)


@ext(handler=eh, op="get_public_access_block")
def get_public_access_block(cdef):
    bucket_name = eh.state.get("bucket_name")

    desired_public_access_block = get_public_access_block_from_cdef(cdef)

    try:
        response = s3.get_public_access_block(Bucket=bucket_name)
        eh.add_log("Got Public Access Block", response)
    except ClientError as e:
        if e.response['Error']['Code'] == "NoSuchPublicAccessBlockConfiguration":
            response = {}
            eh.add_log("No Public Access Block", response)
        else:
            handle_common_errors(e, eh, "Get Public Access Block Error", 12)
            return
    
    if response.get("PublicAccessBlockConfiguration") != desired_public_access_block:
        if desired_public_access_block:
            eh.add_op("set_public_access_block")
        else:
            eh.add_op("delete_public_access_block")


@ext(handler=eh, op="set_public_access_block")
def set_public_access_block(cdef):
    bucket_name = eh.state.get("bucket_name")

    public_access_block = get_public_access_block_from_cdef(cdef)

    try:
        _ = s3.put_public_access_block(Bucket=bucket_name, PublicAccessBlockConfiguration=public_access_block)
        eh.add_log("Put Public Access Block", public_access_block)

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "BucketAlreadyExists":
            eh.perm_error(f"Bucket {bucket_name} Already Exists", 0)
            eh.add_log("Bucket Already Exists, Exiting", {"error": str(e)}, is_error=True)
            return None
        elif e.response['Error']['Code'] == "BucketAlreadyOwnedByYou":
            eh.add_log("Bucket Already in this Account", {"error": str(e)})
            return None
        else:
            print(str(e))
            eh.retry_error("Public Access Block Error", 15)
            eh.add_log("Public Access Block Error", {"error": str(e)}, is_error=True)

@ext(handler=eh, op="delete_public_access_block")
def delete_public_access_block():
    bucket_name = eh.state.get("bucket_name")

    try:
        _ = s3.delete_public_access_block(Bucket=bucket_name)
        eh.add_log("Delete Public Access Block", {"bucket_name": bucket_name})

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "BucketAlreadyExists":
            eh.perm_error(f"Bucket {bucket_name} Already Exists", 0)
            eh.add_log("Bucket Already Exists, Exiting", {"error": str(e)}, is_error=True)
            return None
        elif e.response['Error']['Code'] == "BucketAlreadyOwnedByYou":
            eh.add_log("Bucket Already in this Account", {"error": str(e)})
            return None
        else:
            print(str(e))
            eh.retry_error("Public Access Block Error", 15)
            eh.add_log("Public Access Block Error", {"error": str(e)}, is_error=True)

@ext(handler=eh, op="get_bucket_policy")
def get_bucket_policy(cdef):
    bucket_name = eh.state.get("bucket_name")

    desired_bucket_policy = json.dumps(render_bucket_policy(cdef.get("bucket_policy") or {}, bucket_name), sort_keys=True) if cdef.get("bucket_policy") else ""
    print(f"Rendered Bucket Policy: {desired_bucket_policy}")
    try:
        response = s3.get_bucket_policy(Bucket=bucket_name)
        eh.add_log("Got Bucket Policy", response)
        bucket_policy = json.dumps(json.loads(response.get("Policy")), sort_keys=True)

    except ClientError as e:
        if e.response['Error']['Code'] == "NoSuchBucketPolicy":
            bucket_policy = ""
            eh.add_log("No Bucket Policy", {"bucket_name": bucket_name})
        else:
            handle_common_errors(e, eh, "Get Bucket Policy Error", 20)
            return

    if bucket_policy != desired_bucket_policy:
        if desired_bucket_policy:
            eh.add_op("set_bucket_policy")
        else:
            eh.add_op("delete_bucket_policy")

@ext(handler=eh, op="set_bucket_policy")
def set_bucket_policy(cdef):
    bucket_name = eh.state.get("bucket_name")

    bucket_policy = render_bucket_policy(cdef.get("bucket_policy") or {}, bucket_name)
    try:
        response = s3.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps(bucket_policy))
        eh.add_log("Put Bucket Policy", response)

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "BucketAlreadyExists":
            eh.perm_error(f"Bucket {bucket_name} Already Exists", 0)
            eh.add_log("Bucket Already Exists, Exiting", {"error": str(e)}, is_error=True)
            return None
        elif e.response['Error']['Code'] == "BucketAlreadyOwnedByYou":
            eh.add_log("Bucket Already in this Account", {"error": str(e)})
            return None
        else:
            print(str(e))
            eh.retry_error("Bucket Policy Error", 30)
            eh.add_log("Bucket Policy Error", {"error": str(e)}, is_error=True)

@ext(handler=eh, op="delete_bucket_policy")
def delete_bucket_policy():
    bucket_name = eh.state.get("bucket_name")

    try:
        response = s3.delete_bucket_policy(Bucket=bucket_name)
        eh.add_log("Delete Bucket Policy", response)

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "BucketAlreadyExists":
            eh.perm_error(f"Bucket {bucket_name} Already Exists", 0)
            eh.add_log("Bucket Already Exists, Exiting", {"error": str(e)}, is_error=True)
            return None
        elif e.response['Error']['Code'] == "BucketAlreadyOwnedByYou":
            eh.add_log("Bucket Already in this Account", {"error": str(e)})
            return None
        else:
            print(str(e))
            eh.retry_error("Bucket Policy Error", 30)
            eh.add_log("Bucket Policy Error", {"error": str(e)}, is_error=True)


@ext(handler=eh, op="set_versioning")
def set_versioning(cdef):
    bucket_name = eh.state.get("bucket_name")

    versioning = cdef.get("versioning")
    config = {"Status": "Enabled" if versioning else "Suspended"}

    try:
        response = s3.put_bucket_versioning(Bucket=bucket_name, VersioningConfiguration=config)
        _ = response.pop("ResponseMetadata")
        eh.add_log("Put Bucket Versioning", response)

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "BucketAlreadyExists":
            eh.perm_error(f"Bucket {bucket_name} Already Exists", 0)
            eh.add_log("Bucket Already Exists, Exiting", {"error": str(e)}, is_error=True)
            return None
        elif e.response['Error']['Code'] == "BucketAlreadyOwnedByYou":
            eh.add_log("Bucket Already in this Account", {"error": str(e)})
            return None
        else:
            print(str(e))
            eh.retry_error("Bucket Versioning Error", 50)
            eh.add_log("Bucket Versioning Error", {"error": str(e)}, is_error=True)

@ext(handler=eh, op="get_bucket_cors")
def get_cors(cdef):
    bucket_name = eh.state.get("bucket_name")

    desired_cors_config = get_cors_config_from_cdef(cdef)
    print(f"Desired CORS Config: {desired_cors_config}")
    try:
        cors_config = s3.get_bucket_cors(Bucket=bucket_name)
        eh.add_log("Got Bucket CORS", cors_config)
    except ClientError as e:
        if e.response['Error']['Code'] == "NoSuchCORSConfiguration":
            cors_config = None
            eh.add_log("No Bucket CORS", {"bucket_name": bucket_name})
        else:
            handle_common_errors(e, eh, "Get Bucket CORS Error", 60)
            return

    print(f"Current CORS Config: {cors_config}")
    if cors_config != desired_cors_config:
        if desired_cors_config:
            eh.add_op("set_cors")
        else:
            eh.add_op("delete_cors")


@ext(handler=eh, op="set_cors")
def set_cors(cdef):
    bucket_name = eh.state.get("bucket_name")

    cors_config = get_cors_config_from_cdef(cdef)

    try:
        response = s3.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)
        eh.add_log("Put Bucket CORS", response)

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "BucketAlreadyExists":
            eh.perm_error(f"Bucket {bucket_name} Already Exists", 0)
            eh.add_log("Bucket Already Exists, Exiting", {"error": str(e)}, is_error=True)
            return None
        elif e.response['Error']['Code'] == "BucketAlreadyOwnedByYou":
            eh.add_log("Bucket Already in this Account", {"error": str(e)})
            return None
        else:
            print(str(e))
            eh.retry_error("Bucket CORS Error", 65)
            eh.add_log("Bucket CORS Error", {"error": str(e)}, is_error=True)

@ext(handler=eh, op="delete_cors")
def delete_cors():
    bucket_name = eh.state.get("bucket_name")

    try:
        response = s3.delete_bucket_cors(Bucket=bucket_name)
        eh.add_log("Delete Bucket CORS", response)

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "BucketAlreadyExists":
            eh.perm_error(f"Bucket {bucket_name} Already Exists", 0)
            eh.add_log("Bucket Already Exists, Exiting", {"error": str(e)}, is_error=True)
            return None
        elif e.response['Error']['Code'] == "BucketAlreadyOwnedByYou":
            eh.add_log("Bucket Already in this Account", {"error": str(e)})
            return None
        else:
            print(str(e))
            eh.retry_error("Bucket CORS Error", 65)
            eh.add_log("Bucket CORS Error", {"error": str(e)}, is_error=True)

@ext(handler=eh, op="set_tags")
def set_tags():
    bucket_name = eh.state.get("bucket_name")

    formatted_tags = format_tags(eh.ops['set_tags'])

    try:
        _ = s3.put_bucket_tagging(
            Bucket=bucket_name,
            Tagging={"TagSet": formatted_tags}
        )
        eh.add_log("Tags Set", {"tags": formatted_tags})

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] in ["ResourceInUseException", "LimitExceededException"]:
            eh.retry_error("Tag Limit", 80)
            return None
        else:
            print(str(e))
            eh.retry_error("Bucket Tags Error", 80)
            eh.add_log("Bucket Tags Error", {"error": str(e)}, is_error=True)

@ext(handler=eh, op="remove_all_tags")
def remove_all_tags():
    bucket_name = eh.state.get("bucket_name")

    try:
        _ = s3.delete_bucket_tagging(Bucket=bucket_name)
        eh.add_log("Tags Deleted", {"bucket_name": bucket_name})

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] in ["ResourceInUseException", "LimitExceededException"]:
            eh.retry_error("Tag Limit", 80)
            return None
        else:
            print(str(e))
            eh.retry_error("Bucket Tags Error", 80)
            eh.add_log("Bucket Tags Error", {"error": str(e)}, is_error=True)

@ext(handler=eh, op="put_bucket_website")
def put_bucket_website(cdef):
    bucket_name = eh.state.get("bucket_name")

    website_config = cdef.get("website_configuration")
    print(f"website_config = {website_config}")

    params = remove_none_attributes({
        "ErrorDocument": remove_none_attributes({"Key": website_config.get("error_document")}) or None,
        "IndexDocument": remove_none_attributes({"Suffix": website_config.get("index_document")}) or None,
        "RedirectAllRequestsTo": remove_none_attributes({
            "HostName": website_config.get("redirect_to"),
            "Protocol": website_config.get("redirect_protocol")
        }) or None,
        "RoutingRules": website_config.get("routing_rules")
    })

    print(f"params = {params}")

    try:
        _ = s3.put_bucket_website(
            Bucket=bucket_name,
            WebsiteConfiguration=params
        )
        eh.add_log("Website Config Set", {"config": website_config})

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "BucketAlreadyExists":
            eh.perm_error(f"Bucket {bucket_name} Already Exists", 90)
            eh.add_log("Bucket Already Exists, Exiting", {"error": str(e)}, is_error=True)
            return None
        elif e.response['Error']['Code'] == "BucketAlreadyOwnedByYou":
            eh.add_log("Bucket Already in this Account", {"error": str(e)})
            return None
        else:
            print(str(e))
            eh.retry_error("Bucket Website Error", 90)
            eh.add_log("Bucket Website Error", {"error": str(e)}, is_error=True)

@ext(handler=eh, op="delete_bucket_website")
def delete_bucket_website():
    bucket_name = eh.state.get("bucket_name")

    try:
        _ = s3.delete_bucket_website(Bucket=bucket_name)
        eh.add_log("Website Config Deleted", {"bucket_name": bucket_name})

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "BucketAlreadyExists":
            eh.perm_error(f"Bucket {bucket_name} Already Exists", 90)
            eh.add_log("Bucket Already Exists, Exiting", {"error": str(e)}, is_error=True)
            return None
        elif e.response['Error']['Code'] == "BucketAlreadyOwnedByYou":
            eh.add_log("Bucket Already in this Account", {"error": str(e)})
            return None
        else:
            print(str(e))
            eh.retry_error("Bucket Website Error", 90)
            eh.add_log("Bucket Website Error", {"error": str(e)}, is_error=True)

@ext(handler=eh, op="get_bucket_encryption")
def get_bucket_encryption(cdef):
    bucket_name = eh.state.get("bucket_name")

    desired_encryption = {
        "ServerSideEncryptionConfiguration": {
            "Rules": [
                {
                    "ApplyServerSideEncryptionByDefault": remove_none_attributes({
                        "SSEAlgorithm": "aws:kms" if cdef.get("default_kms_key_id") else "AES256",
                        "KMSMasterKeyID": cdef.get("default_kms_key_id")
                    }),
                    "BucketKeyEnabled": cdef.get("bucket_key_enabled", bool(cdef.get("default_kms_key_id")))
                }
            ]
        }
    }

    try:
        encryption = s3.get_bucket_encryption(Bucket=bucket_name)
        eh.add_log("Got Bucket Encryption", {"encryption": encryption})

        if encryption['ServerSideEncryptionConfiguration'] != desired_encryption['ServerSideEncryptionConfiguration']:
            eh.add_op("set_bucket_encryption", desired_encryption)

    except ClientError as e:
        handle_common_errors(e, eh, "Get Bucket Encryption Failure", 91)

@ext(handler=eh, op="set_bucket_encryption")
def set_bucket_encryption():
    bucket_name = eh.state.get("bucket_name")
    encryption = eh.ops['set_bucket_encryption']

    try:
        _ = s3.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration=encryption['ServerSideEncryptionConfiguration']
        )
        eh.add_log("Set Bucket Encryption", {"encryption": encryption})

    except ClientError as e:
        handle_common_errors(e, eh, "Set Bucket Encryption Failure", 92)


@ext(handler=eh, op="delete_bucket")
def delete_bucket():
    bucket_name = eh.state.get("bucket_name")
    print(bucket_name)
    try:
        s3 = boto3.resource('s3')
        s3_bucket = s3.Bucket(bucket_name)
        bucket_versioning = s3.BucketVersioning(bucket_name)
        if bucket_versioning.status == 'Enabled':
            s3_bucket.object_versions.delete()
            eh.add_log("Deleted All Object Versions", {"bucket_name": bucket_name})
        else:
            s3_bucket.objects.all().delete()
            eh.add_log("Deleted All Objects", {"bucket_name": bucket_name})

        s3_bucket.delete()
        eh.add_log("Deleted Bucket", {"bucket_name": bucket_name})

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "NoSuchBucket":
            eh.add_log("Bucket Does Not Exist, Continuing")
        else:
            print(str(e))
            eh.retry_error(str(e))
        

def gen_bucket_arn(bucket_name):
    return f"arn:aws:s3:::{bucket_name}"

def gen_bucket_link(bucket_name):
    return f"https://s3.console.aws.amazon.com/s3/buckets/{bucket_name}?region=us-east-1&tab=objects"

def render_bucket_policy(policy, bucket_name):
    print(f"policy = {policy}, bucket_name = {bucket_name}")
    arn = gen_bucket_arn(bucket_name)
    print(f"ARN = {arn}")
    def update_f(string):
        return string.replace("$SELF$", arn).replace("#SELF#", arn)

    return generic_render_def(policy, update_f)

def generic_render_def(obj, update_f):
    retval = {}
    if isinstance(obj, dict):
        return {k: generic_render_def(v, update_f) for k,v in obj.items()}
    elif isinstance(obj, (set, list, tuple)):
        return [generic_render_def(v, update_f) for v in obj]
    elif isinstance(obj, str):
        return update_f(obj)
    else:
        return obj
    

def generate_alternate_bucket_name():
    return f"ck-{random_id()}"

def get_acl_params_from_cdef(cdef):
    if cdef.get("public_read") == True:
        params = {
            "GrantRead": "uri=http://acs.amazonaws.com/groups/global/AllUsers"
        }

    elif cdef.get("acl"):
        params = cdef.get("acl") or {}

    else:
        params = {"ACL": "private"}

    return params

def get_public_access_block_from_cdef(cdef):
    if (cdef.get("block_public_access") == True or cdef.get("public_access_block") == True):
        desired_public_access_block = {
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True
        }
    elif (cdef.get("block_public_access") == False) or (cdef.get("public_access_block") == False):
        desired_public_access_block = {
            "BlockPublicAcls": False,
            "IgnorePublicAcls": False,
            "BlockPublicPolicy": False,
            "RestrictPublicBuckets": False
        }
    
    else:
        desired_public_access_block = cdef.get("public_access_block")

    return desired_public_access_block

def get_cors_config_from_cdef(cdef):
    if cdef.get("CORS") == True:
        cors_config = {"CORSRules": [
            {
                'AllowedHeaders': ['*'],
                'AllowedMethods': ['PUT', 'DELETE', 'GET', 'POST', 'HEAD'],
                'AllowedOrigins': ['*'],
                'ExposeHeaders': ['PUT', 'DELETE', 'GET', 'POST', 'HEAD'],
                'MaxAgeSeconds': 3000
            }
        ]}

    else:
        cors_config = cdef.get("CORS")
    
    return cors_config

def get_grantee_from_grant_string(grant_string):
    key, grantee = grant_string.split("=")
    if key == "id":
        key = "ID"
    elif key == "email":
        key = "EmailAddress"
    elif key == "uri":
        key = "URI"
    return {key: grantee}