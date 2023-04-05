import boto3

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        object_key = record['s3']['object']['key']
        # Check if the file doesn't already have the .csv extension
        if not object_key.endswith('.csv'):
            # Rename the file with the .csv extension
            new_key = object_key + '.csv'
            print(f"{bucket_name}/{object_key}")
            s3.copy_object(Bucket=bucket_name, CopySource=f"{bucket_name}/{object_key}", Key=new_key)
            s3.delete_object(Bucket=bucket_name, Key=object_key)
            # s3.delete_object(Bucket=bucket_name, Key=object_key.replace("write/", "read/"))
