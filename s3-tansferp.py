import boto3
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError

def validate_bucket(bucket_name):
    s3 = boto3.client('s3')
    try:
        s3.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as e:
        print(f"❌ Bucket '{bucket_name}' is invalid or not accessible: {e.response['Error']['Message']}")
        return False

def copy_s3_objects(source_bucket, destination_bucket, prefix=''):
    if not validate_bucket(source_bucket) or not validate_bucket(destination_bucket):
        print("❌ Cannot proceed due to invalid bucket(s).")
        return

    s3 = boto3.resource('s3')
    source = s3.Bucket(source_bucket)

    try:
        for obj in source.objects.filter(Prefix=prefix):
            copy_source = {
                'Bucket': source_bucket,
                'Key': obj.key
            }
            print(f"➡️ Copying {obj.key} to {destination_bucket}...")
            s3.Object(destination_bucket, obj.key).copy(copy_source)
        print("✅ All files copied successfully.")
    except NoCredentialsError:
        print("❌ AWS credentials not found.")
    except EndpointConnectionError as e:
        print(f"❌ Network error: {e}")
    except ClientError as e:
        print(f"❌ AWS error: {e.response['Error']['Message']}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

# Example usage
source_bucket = 'your-source-bucket'
destination_bucket = 'your-destination-bucket'
copy_s3_objects(source_bucket, destination_bucket)
