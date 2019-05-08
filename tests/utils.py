import boto3

from io import BytesIO


def load_fileobj(s3_bucket_name, key):
    s3_client = boto3.client("s3")
    bytes_obj = BytesIO()
    s3_client.download_fileobj(s3_bucket_name, key, bytes_obj)
    return bytes_obj


def generate_fileobj_from_s3_folder(s3_bucket_name, s3_folder_key):
    s3 = boto3.resource("s3")
    s3_bucket = s3.Bucket(name=s3_bucket_name)
    for obj in s3_bucket.objects.filter(Prefix=s3_folder_key):
        if obj.key == s3_folder_key:
            continue
        bytes_obj = load_fileobj(obj.bucket_name, obj.key)
        yield obj.key, bytes_obj
