import boto3
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError

def copy_s3_objects(source_bucket, destination_bucket, prefix=''):
    s3 = boto3.resource('s3')
    source = s3.Bucket(source_bucket)

    try:
        for obj in source.objects.filter(Prefix=prefix):
            copy_source = {
                'Bucket': source_bucket,
                'Key': obj.key
            }
            print(f"Copying {obj.key} to {destination_bucket}...")
            s3.Object(destination_bucket, obj.key).copy(copy_source)
        print("✅ All files copied successfully.")
    except NoCredentialsError:
        print("❌ AWS credentials not found. Please configure them.")
    except EndpointConnectionError as e:
        print(f"❌ Network error: {e}")
    except ClientError as e:
        print(f"❌ AWS error: {e.response['Error']['Message']}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

# Example usage
source_bucket = 'my-source-bucket-name'
destination_bucket = 'my-destination-bucket-name'

copy_s3_objects(source_bucket, destination_bucket)
