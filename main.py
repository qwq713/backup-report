import sys
import subprocess

from module import client
from module import data_if
from module import excel


if __name__ == "__main__":
    arguments = sys.argv

    if len(arguments) == 1 :
        exit("Please Enter your ProfileName")
    
    profile_name = arguments[1]
    
    list_profiles = subprocess.run(["aws","configure","list-profiles"],capture_output=True).stdout.decode("utf-8").split("\n")

    if profile_name not in list_profiles:
        exit("The profile you entered does not exist. Please Check your entered profile again.")
    
    auth_dict = {"profile":profile_name}

    ec2_client = client.get_client(auth_dict,client_name="ec2")
    rds_client = client.get_client(auth_dict,client_name="rds")
    aws_backup_client = client.get_client(auth_dict,client_name="backup")
    fsx_client = client.get_client(auth_dict,client_name="fsx")
    efs_client = client.get_client(auth_dict,client_name="efs")
    sts_client = client.get_client(auth_dict,client_name="sts")

    account_id = sts_client.get_caller_identity()["Account"]

    # EC2
    ec2_header , ec2_rows = data_if.ec2_backup_data(account_id,ec2_client)

    # RDS
    rds_header , rds_rows = data_if.rds_backup_data(account_id,rds_client)

    # EFS 
    backup_vault_name = "aws/efs/automatic-backup-vault"
    efs_header , efs_rows = data_if.efs_backup_data(account_id,efs_client,aws_backup_client,backup_vault_name)
        
    # FSX
    fsx_header, fsx_rows = data_if.fsx_backup_data(account_id,fsx_client)

    wb = excel.make_workbook()
    wb = excel.attach_sheet(wb,'EC2',ec2_header,ec2_rows)
    wb = excel.attach_sheet(wb,'RDS',rds_header,rds_rows)
    wb = excel.attach_sheet(wb,'EFS',efs_header,efs_rows)
    wb = excel.attach_sheet(wb,'FSX',fsx_header,fsx_rows)
    result = excel.make_excel_file(workbook=wb,account_id=account_id)
    
    
    









