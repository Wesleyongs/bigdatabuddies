import json
    import boto3
    from datetime import datetime, timedelta, timezone
    
    def lambda_handler(event, context):
        
        # Get the S3 bucket and key from the event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']
        
        # Copy the CSV file to the processing folder in the same bucket
        destination_object_key = 'processing/' + object_key[5:]
        s3 = boto3.client('s3')
        print(bucket_name)
        print(object_key)
        print(destination_object_key)
        s3.copy_object(
            CopySource={'Bucket': bucket_name, 'Key': object_key},
            Bucket=bucket_name,
            Key=destination_object_key
        )
        print(f'Copied file {object_key} to {destination_object_key}')
        
        # Get the objects in the bucket and prefix
        prefix = "processing/"
        objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)['Contents']
        print(objects)
        
        # Get the current time and the cutoff time
        current_time = datetime.now(timezone.utc).replace(microsecond=0)
        cutoff_time = current_time - timedelta(minutes=15)
        last_modified_objects = [(obj['LastModified'].replace(tzinfo=timezone.utc).replace(microsecond=0), obj['Key']) for obj in objects]
    
        # Iterate through the objects and delete those older than the cutoff time
        for last_modified, object_key in last_modified_objects:
            if last_modified < cutoff_time:
                s3.delete_object(Bucket=bucket_name, Key=object_key)
                print(f"Deleted object {object_key}")
        
         # Start the AWS Glue crawler
        glue = boto3.client('glue')
        if bucket_name == "is459-twitter-preprocessed":
            response = glue.start_job_run(JobName='handsome job')
        else:
            response = glue.start_job_run(JobName='handsome job')
    
        print(response)
        return 'Job started'