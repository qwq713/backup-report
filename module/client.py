import boto3

def get_client(auth_dict:dict,client_name:str):
    
    if not auth_dict:
        return boto3.client(client_name)    
    
    elif auth_dict.get("profile",False):
        session = boto3.Session(profile_name=auth_dict.get("profile"))
        
    else:    
        aws_access_key_id = auth_dict.get("aws_access_key_id",False)
        aws_secret_access_key = auth_dict.get("aws_secret_access_key",False)
        region_name = auth_dict.get("region_name",False)
        session = boto3.Session(aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key,region_name=region_name)
    
    return session.client(client_name)