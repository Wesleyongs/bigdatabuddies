import boto3
from datetime import datetime, timedelta, timezone

def lambda_handler(event, context):
    # Define the S3 bucket and prefix
    bucket_name = 'handsome-bucket-reddit'
    prefix = 'processing/'

    # Create an S3 client
    s3 = boto3.client('s3')

    # Get the objects in the bucket and prefix
    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)['Contents']

    # Get the current time and the cutoff time
    current_time = datetime.now(timezone.utc).replace(microsecond=0)
    cutoff_time = current_time - timedelta(minutes=15)
    last_modified_objects = [(obj['LastModified'].replace(tzinfo=timezone.utc).replace(microsecond=0), obj['Key']) for obj in objects]

    # Iterate through the objects and delete those older than the cutoff time
    for last_modified, object_key in last_modified_objects:
        if last_modified < cutoff_time:
            s3.delete_object(Bucket=bucket_name, Key=object_key)
            print(f"Deleted object {object_key}")
