import boto3
from botocore.exceptions import ClientError, NoCredentialsError

def destination_prefix_exists(bucket_name, prefix):
    """Check if the prefix exists in the destination bucket."""
    if not prefix.endswith('/'):
        prefix = '/'.join(prefix.split('/')[:-1]) + '/'
    s3 = boto3.client('s3')
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=1)
        return 'Contents' in response
    except ClientError as e:
        print(f"❌ Error checking destination prefix: {e.response['Error']['Message']}")
        return False

def copy_s3_objects(source_bucket, destination_bucket, prefix='', dest_prefix=''):
    s3 = boto3.resource('s3')

    try:
        # Check source/destination bucket access
        s3.meta.client.head_bucket(Bucket=source_bucket)
        s3.meta.client.head_bucket(Bucket=destination_bucket)
    except ClientError as e:
        print(f"❌ Error accessing buckets: {e.response['Error']['Message']}")
        return

    if not destination_prefix_exists(destination_bucket, dest_prefix):
        print(f"❌ Destination prefix '{dest_prefix}' does not exist in bucket '{destination_bucket}'.")
        return

    try:
        for obj in s3.Bucket(source_bucket).objects.filter(Prefix=prefix):
            file_name = obj.key.split('/')[-1]
            destination_key = f"{dest_prefix.rstrip('/')}/{file_name}"
            print(f"➡️ Copying {obj.key} → {destination_key}")
            s3.Object(destination_bucket, destination_key).copy_from(
                CopySource={'Bucket': source_bucket, 'Key': obj.key}
            )
        print("✅ All objects copied.")
    except NoCredentialsError:
        print("❌ AWS credentials not found.")
    except ClientError as e:
        print(f"❌ AWS error: {e.response['Error']['Message']}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

# 🔧 Example usage
source_bucket = 'my-test-bucket-rgangwar-1745337465'
destination_bucket = 'my-test-bucket-rgangwar-new-1745337671'
prefix_to_copy = 'test/policy.json'
destination_prefix = 'tedsadt-ne/policy.json'

copy_s3_objects(source_bucket, destination_bucket, prefix_to_copy, destination_prefix)
