from module import describe


def ec2_backup_data(account_id, ec2_client):
    ec2_header = ['AccountId', 'Name', 'Service', 'PrivateIpAddress',
                  'InstanceId', 'InstanceType', 'AmiCount', 'LatestAmiCreationDate']
    ec2_rows = []

    ec2_instances = describe.all_ec2_instances(ec2_client=ec2_client)
    ami_list = describe.all_ami_images(
        ec2_client=ec2_client, owner_id=account_id, prefix="AwsBackup_")

    ami_map = {}
    for ami in ami_list:
        name = ami.get("Name")
        instance_id = name[len("AwsBackup_"):len("AwsBackup_")+19]
        creation_date = ami.get("CreationDate")[:10]
        if ami_map.get(instance_id, False):
            ami_map[instance_id]['AmiCount'] += 1
            ami_map[instance_id]['LatestAmiCreationDate'] = max(
                ami_map[instance_id]['LatestAmiCreationDate'], creation_date)
        else:
            ami_map[instance_id] = {}
            ami_map[instance_id]['AmiCount'] = 1
            ami_map[instance_id]['LatestAmiCreationDate'] = creation_date

    for ec2 in ec2_instances:
        tags = ec2.get("Tags", [])
        name = [tag['Value'] for tag in tags if tag['Key'] == 'Name']
        service = [tag['Value'] for tag in tags if tag['Key'] == 'Service']
        name = name[0] if name else "None"
        service = service[0] if service else "None"
        private_ip = ec2["PrivateIpAddress"]
        instance_id = ec2["InstanceId"]
        instance_type = ec2["InstanceType"]

        ami_info = ami_map.get(instance_id, {})
        ami_count = ami_info.get("AmiCount", 0)
        latest_ami_creation_date = ami_info.get("LatestAmiCreationDate", 0)

        ec2_rows.append([account_id, name, service, private_ip, instance_id,
                        instance_type, ami_count, latest_ami_creation_date])
    return ec2_header, ec2_rows


def rds_backup_data(account_id, rds_client):
    rds_list = describe.all_rds(rds_client=rds_client)
    rds_snap_list = describe.all_rds_snaps(rds_client=rds_client)
    rds_cluster_list = describe.all_rds_clusters(rds_client=rds_client)
    rds_cluster_snap_list = describe.all_rds_cluster_snaps(
        rds_client=rds_client)

    rds_header = ["AccountId", "DBInstanceIdentifier", "Service",
                  "Engine", "EngineVersion", "SnapCount", "LatestSnapshotCreateTime"]
    rds_rows = []

    rds_snap_map = {}

    for rds_snap in rds_snap_list:
        db_identifier = rds_snap["DBInstanceIdentifier"]

        creation_time = rds_snap["SnapshotCreateTime"].strftime("%Y-%m-%d")

        if rds_snap_map.get(db_identifier, False):
            rds_snap_map[db_identifier]["SnapCount"] += 1
            rds_snap_map[db_identifier]["LatestSnapshotCreateTime"] = max(
                rds_snap_map[db_identifier]["LatestSnapshotCreateTime"], creation_time)
        else:
            rds_snap_map[db_identifier] = {}
            rds_snap_map[db_identifier]["SnapCount"] = 1
            rds_snap_map[db_identifier]["LatestSnapshotCreateTime"] = creation_time

    for rds_cluster_snap in rds_cluster_snap_list:
        db_cluster_identifier = rds_cluster_snap["DBClusterIdentifier"]

        creation_time = rds_cluster_snap["SnapshotCreateTime"].strftime(
            "%Y-%m-%d")

        if rds_snap_map.get(db_cluster_identifier, False):
            rds_snap_map[db_cluster_identifier]["SnapCount"] += 1
            rds_snap_map[db_cluster_identifier]["LatestSnapshotCreateTime"] = max(
                rds_snap_map[db_cluster_identifier]["LatestSnapshotCreateTime"], creation_time)
        else:
            rds_snap_map[db_cluster_identifier] = {}
            rds_snap_map[db_cluster_identifier]["SnapCount"] = 1
            rds_snap_map[db_cluster_identifier]["LatestSnapshotCreateTime"] = creation_time

    for rds in rds_list:

        if rds.get("DBClusterIdentifier", False):
            db_identifier = rds["DBClusterIdentifier"]
        else:
            db_identifier = rds["DBInstanceIdentifier"]

        tags = rds.get("TagList", [])
        service = [tag['Value'] for tag in tags if tag["Key"] == 'Service']
        if service:
            service = service[0]
        else:
            service = "None"

        engine = rds["Engine"]
        engine_version = rds["EngineVersion"]
        snap_info = rds_snap_map.get(db_identifier, {})
        snap_count = snap_info.get("SnapCount", 0)
        latest_snapshot_creation_time = snap_info.get(
            "LatestSnapshotCreateTime", 0)

        rds_rows.append([account_id, db_identifier, service, engine,
                        engine_version, snap_count, latest_snapshot_creation_time])

    return rds_header, rds_rows


def efs_backup_data(account_id,efs_client,aws_backup_client,backup_vault_name):
    efs_list = describe.all_efs(efs_client)
    efs_backup_list = describe.all_recovery_points(aws_backup_client,backup_vault_name)

    efs_header = ["AccountId","Name","FileSystemId","Service","NumberOfMountTargets","PerformanceMode","BackupCount","LatestCompletionDate"]
    efs_rows = []

    efs_backup_map = {}

    for efs_backup in efs_backup_list:
        resource_arn = efs_backup["ResourceArn"]
        file_system_id = resource_arn[resource_arn.rfind("/")+1:]
        completion_date = efs_backup["CompletionDate"].strftime("%Y-%m-%d")
        
        if efs_backup_map.get(file_system_id,False):
            efs_backup_map[file_system_id]["BackupCount"] += 1
            efs_backup_map[file_system_id]["LatestCompletionDate"] = max(efs_backup_map[file_system_id]["LatestCompletionDate"],completion_date)
        else:
            efs_backup_map[file_system_id] = {}
            efs_backup_map[file_system_id]["BackupCount"] = 1
            efs_backup_map[file_system_id]["LatestCompletionDate"] = completion_date
        
    for efs in efs_list:
        name = efs["Name"]
        file_system_id = efs["FileSystemId"]
        tags = efs.get("Tags",[])
        service = [ tag["Value"] for tag in tags if tag["Key"] == "Service"]
        if service:
            service = service[0]
        else:
            service = "None"
        number_of_mount_targets = efs["NumberOfMountTargets"]
        performance_mode = efs["PerformanceMode"]
        backup_info = efs_backup_map.get(file_system_id,{})
        backup_count = backup_info.get("BackupCount",0)
        latest_completion_date = backup_info.get("LatestCompletionDate",0)
        
        efs_rows.append([account_id,name,file_system_id,service,number_of_mount_targets,performance_mode,backup_count,latest_completion_date])
    
    return efs_header, efs_rows

def fsx_backup_data(account_id,fsx_client):
    fsx_list = describe.all_fsx(fsx_client=fsx_client)
    fsx_backup_list = describe.all_fsx_backups(fsx_client=fsx_client)

    fsx_header = ["AccountId","Name","FileSystemId","Service","StorageCapacity","StorageType","BackupCount","LatestCreationTime"]
    fsx_rows = []

    fsx_backup_map = {}

    for fsx_backup in fsx_backup_list:
        file_system = fsx_backup.get("FileSystem",{})
        file_system_id = file_system["FileSystemId"]
        creation_time = fsx_backup["CreationTime"].strftime("%Y-%m-%d")
        
        if fsx_backup_map.get(file_system_id,False):
            fsx_backup_map[file_system_id]["BackupCount"] += 1
            fsx_backup_map[file_system_id]["LatestCreationTime"] = max(fsx_backup_map[file_system_id]["LatestCreationTime"],creation_time)
        else:
            fsx_backup_map[file_system_id] = {}
            fsx_backup_map[file_system_id]["BackupCount"] = 1
            fsx_backup_map[file_system_id]["LatestCreationTime"] = creation_time

    for fsx in fsx_list:
        tags = fsx.get("Tags",[])
        name = [ tag["Value"] for tag in tags if tag["Key"] == "Name"]
        service = [ tag["Value"] for tag in tags if tag["Key"] == "Service"]
        
        if name:
            name = name[0]
        else:
            name = "None"
            
        if service:
                service = service[0]
        else:
            service = "None"
            
        file_system_id = fsx["FileSystemId"]
        storage_capacity = fsx["StorageCapacity"]
        storage_type = fsx["StorageType"]
        backup_info = fsx_backup_map.get(file_system_id,{})
        backup_count = backup_info.get("BackupCount",0)
        latest_creation_time = backup_info.get("LatestCreationTime",0)
        fsx_rows.append([account_id,name,file_system_id,service,storage_capacity,storage_type,backup_count,latest_creation_time])
    
    return fsx_header ,fsx_rows