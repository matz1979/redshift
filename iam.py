import boto3
import pandas as import pd
import json 

redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

try:
    response = redshift.create_cluster(
        #HW
        ClusterType= multi-node,
        NodeType= dc2.large,
        NumberOfNodes=int(4),

        #Identifiers & Credentials
        DBName= matthias,
        ClusterIdentifier= myCluster,
        MasterUsername= DB_user,
        MasterUserPassword= Bianca1980,

        #Roles (for s3 access)
        IamRoles=['arn:aws:iam::999115771860:user/matthias']
    )
except Exception as e:
    print(e)


def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus",
                  "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k, v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


myClusterProps = redshift.describe_clusters(
    ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
prettyRedshiftProps(myClusterProps)
