from typing import *

def all_load_balancers(elb_client):
    result = []
    response = elb_client.describe_load_balancers()
    load_balancers = response.get("LoadBalancers", [])
    result.extend(load_balancers)
    next_maker = response.get("NextMarker", False)

    while next_maker:
        response = elb_client.describe_load_balancers(Marker=next_maker)
        load_balancers = response.get("LoadBalancers", [])
        result.extend(load_balancers)
        next_maker = response.get("NextMarker", False)
    return result


def all_listeners(elb_client,all_load_balancers):
    result = {}
    load_balancer_arn_key = "LoadBalancerArn"
    all_load_balancers_arn = [ load_balancer.get(load_balancer_arn_key)  for load_balancer in all_load_balancers if load_balancer.get("LoadBalancerArn",False)]
    
    for arn in all_load_balancers_arn:
        response = elb_client.describe_listeners(LoadBalancerArn=arn)
        listeners = response.get("Listeners",[])
        result[arn] = listeners
    
    return result

def all_target_groups(elb_client):
    result = []
    response = elb_client.describe_target_groups()
    target_groups = response.get("TargetGroups", [])
    result.extend(target_groups)
    next_marker = response.get("NextMarker", False)

    while next_marker:
        response = elb_client.describe_target_groups(Marker=next_marker)
        target_groups = response.get("TargetGroups", [])
        result.extend(target_groups)
        next_marker = response.get("NextMarker", False)
    return result


def all_target_group_health(elb_client, all_target_groups: List[dict]):
    result = {}

    target_group_arn_key = "TargetGroupArn"
    all_target_group_arns = [target_group.get(
        target_group_arn_key) for target_group in all_target_groups]

    for arn in all_target_group_arns:
        response = elb_client.describe_target_health(TargetGroupArn=arn)
        target_health_descriptions = response.get(
            "TargetHealthDescriptions", [])
        targets = [{'id': target_health.get("Target").get("Id"),
                    'port': target_health.get("Target").get("Port")} for target_health in target_health_descriptions if target_health.get("Target")]
        result[arn] = targets

    return result


def all_ec2_instances(ec2_client):
    result = []
    response = ec2_client.describe_instances()
    next_token = response.get("NextToken",False)
    reservations = response.get("Reservations",False)
    if reservations:
        for reserv in reservations:
            instances = reserv.get("Instances",False)
            if instances:
                for instance in instances:
                    if instance.get("State").get("Name") in ["terminated","shutting-down","pending"]:
                        continue
                    result.append(instance)
    
    while next_token:
        response = ec2_client.describe_instances(NextToken=next_token)
        next_token = response.get("NextToken",False)
        reservations = response.get("Reservations",False)
        if reservations:
            for reserv in reservations:
                instances = reserv.get("Instances",False)
                if instances:
                    for instance in instances:
                        if instance.get("State").get("Name") in ["terminated","shutting-down","pending"]:
                            continue
                        result.append(instance)
    return result

def filtered_targets(targets,ec2_instances):
    fileterd_targets = {}
    instances_id_set = { inst.get('InstanceId',False)  for inst in ec2_instances}
    
    for inst_id,lb_name in targets.items():
        if inst_id in instances_id_set:
            fileterd_targets[inst_id] = lb_name    
        
    return fileterd_targets
    
    
    
def all_rds(rds_client):
    ""
    result = []
    response = rds_client.describe_db_instances()
    db_instances = response.get("DBInstances",[])
    result.extend(db_instances)
    
    next_marker = response.get("Marker",False)
    
    while next_marker:
        response = rds_client.describe_db_instances(Marker=next_marker)
        db_instances = response.get("DBInstances",[])
        result.extend(db_instances)
        next_marker = response.get("Marker",False)
        
    return result


def all_rds_clusters(rds_client):
    result = []
    response = rds_client.describe_db_clusters()
    db_clusters = response.get("DBClusters",[])
    result.extend(db_clusters)
    
    next_marker = response.get("Marker",False)
    
    while next_marker:
        response = rds_client.describe_db_clusters(Marker=next_marker)
        db_clusters = response.get("DBClusters",[])
        result.extend(db_clusters)
        
    return result
    
def all_fsx(fsx_client):
    ""
    result = []
    response = fsx_client.describe_file_systems()
    fsx_list = response.get("FileSystems",[])
    result.extend(fsx_list)
    
    next_token = response.get("NextToken",False)
    
    while next_token:
        response = fsx_client.describe_file_systems(NextToken=next_token)
        fsx_list = response.get("FileSystems",[])
        result.extend(fsx_list)
        
        next_token = response.get("NextToken",False)
        
    return result
    
def all_efs(efs_client):
    ""
    result = []
    response = efs_client.describe_file_systems()
    efs_list = response.get("FileSystems",[])
    result.extend(efs_list)
    next_marker = response.get("NextMarker",False)
    
    while next_marker:
        response = efs_client.describe_file_systems(Marker=next_marker)
        efs_list = response.get("FileSystems",[])
        result.extend(efs_list)
        next_marker = response.get("NextMarker",False)
    return result

def all_s3(s3_client):
    ""
    result = []
    response = s3_client.list_buckets()
    buckets = response.get("Buckets",[])
    result.extend(buckets)
    return result
    
    
def all_ami_images(ec2_client,owner_id,prefix):
    response = ec2_client.describe_images(
    Filters=[
        {
            'Name': 'name',
            'Values': [f"{prefix}*"]
        },
    ],
    Owners=[owner_id])
    result = response.get("Images",[])
    return result

def all_rds_snaps(rds_client):
    result = []
    response = rds_client.describe_db_snapshots()
    snaps = response.get("DBSnapshots",[])
    result.extend(snaps)
    next_marker = response.get("Marker",False)
    
    while next_marker:
        response = rds_client.describe_db_snapshots(Marker=next_marker)
        snaps = response.get("DBSnapshots",[])
        result.extend(snaps)
        next_marker = response.get("Marker",False)
        
    return result

def all_rds_cluster_snaps(rds_client):
    result = []
    response = rds_client.describe_db_cluster_snapshots()
    snaps = response.get("DBClusterSnapshots",[])
    result.extend(snaps)
    next_marker = response.get("Marker",False)
    
    while next_marker:
        response = rds_client.describe_db_cluster_snapshots(Marker=next_marker)
        snaps = response.get("DBClusterSnapshots",[])
        result.extend(snaps)
        next_marker = response.get("Marker",False)
    return result


def all_recovery_points(aws_backup_client,backup_valut_name):
    result = []
    
    response = aws_backup_client.list_recovery_points_by_backup_vault(BackupVaultName=backup_valut_name)
    recovery_points = response.get("RecoveryPoints",[])
    result.extend(recovery_points)
    next_token = response.get("NextToken",False)
    
    while next_token:
        response = aws_backup_client.list_recovery_points_by_backup_vault(BackupVaultName=backup_valut_name,NextToken=next_token)
        recovery_points = response.get("RecoveryPoints",[])
        result.extend(recovery_points)
        next_token = response.get("NextToken",False)
    
    return result

def all_fsx_backups(fsx_client):
    result = []
    response = fsx_client.describe_backups()
    fsx_backups = response.get("Backups",[])
    result.extend(fsx_backups)
    next_token = response.get("NextToken",False)
    
    while next_token:
        response = fsx_client.describe_backups(NextToken=next_token)
        fsx_backups = response.get("Backups",[])
        result.extend(fsx_backups)
        next_token = response.get("NextToken",False)
    
    return result

