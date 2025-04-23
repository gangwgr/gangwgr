import boto3
from botocore.exceptions import ClientError, NoCredentialsError

def bucket_exists(bucket_name):
    """Check if an S3 bucket exists and is accessible."""
    s3 = boto3.client('s3')
    try:
        s3.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as e:
        print(f"❌ Bucket '{bucket_name}' check failed: {e.response['Error']['Message']}")
        return False

def destination_prefix_exists(bucket_name, prefix):
    """Check if a given prefix exists in the bucket."""
    if not prefix.endswith('/'):
        prefix = '/'.join(prefix.split('/')[:-1]) + '/'
    s3 = boto3.client('s3')
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=1)
        return 'Contents' in response
    except ClientError as e:
        print(f"❌ Error checking prefix '{prefix}' in bucket '{bucket_name}': {e.response['Error']['Message']}")
        return False

def copy_s3_objects(source_bucket, destination_bucket, prefix='', dest_prefix=''):
    s3 = boto3.resource('s3')

    # Check buckets
    if not bucket_exists(source_bucket) or not bucket_exists(destination_bucket):
        return

    # Check destination prefix
    if not destination_prefix_exists(destination_bucket, dest_prefix):
        print(f"❌ Destination prefix '{dest_prefix}' does not exist in bucket '{destination_bucket}'")
        return

    # Copy objects
    try:
        objects = list(s3.Bucket(source_bucket).objects.filter(Prefix=prefix))
        if not objects:
            print(f"⚠️ No objects found in source bucket '{source_bucket}' with prefix '{prefix}'")
            return

        for obj in objects:
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
        print(f"❌ Unexpected error: {str(e)}")

# 🔧 Example usage
source_bucket = 'my-test-bucket-rgangwar-1745337465'
destination_bucket = 'my-test-bucket-rgangwar-new-1745337671'
prefix_to_copy = 'test/policy.json'
destination_prefix = 'est-ne/'

copy_s3_objects(source_bucket, destination_bucket, prefix_to_copy, destination_prefix)
