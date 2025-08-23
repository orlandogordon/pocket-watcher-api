import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from fastapi import HTTPException, status
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_S3_BUCKET_NAME = os.getenv("AWS_S3_BUCKET_NAME")
AWS_S3_REGION = os.getenv("AWS_S3_REGION", "us-east-1") # Default to us-east-1 if not set

if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_S3_BUCKET_NAME]):
    raise ImportError("AWS credentials or S3 bucket name are not configured in the environment.")

s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_S3_REGION
)

def get_s3_bucket():
    """Returns the configured S3 bucket name."""
    return AWS_S3_BUCKET_NAME

def upload_file_to_s3(file_obj, bucket, object_name):
    """
    Upload a file-like object to an S3 bucket.

    :param file_obj: File-like object to upload.
    :param bucket: Bucket to upload to.
    :param object_name: S3 object name.
    :return: True if file was uploaded, else False
    """
    try:
        s3_client.upload_fileobj(file_obj, bucket, object_name)
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Error with AWS credentials: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="Server is not configured for file uploads."
        ) from e
    except Exception as e:
        print(f"An unexpected error occurred during S3 upload: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="Could not upload file."
        ) from e
    return True

def download_file_from_s3(bucket, object_name, file_obj):
    """
    Download a file from an S3 bucket into a file-like object.

    :param bucket: Bucket to download from.
    :param object_name: S3 object name.
    :param file_obj: File-like object to download into.
    :return: True if file was downloaded, else False
    """
    try:
        s3_client.download_fileobj(bucket, object_name, file_obj)
    except Exception as e:
        print(f"An unexpected error occurred during S3 download: {e}")
        return False
    return True

def delete_file_from_s3(bucket, object_name):
    """
    Delete a file from an S3 bucket.

    :param bucket: Bucket to delete from.
    :param object_name: S3 object name.
    :return: True if file was deleted, else False
    """
    try:
        s3_client.delete_object(Bucket=bucket, Key=object_name)
    except Exception as e:
        print(f"An unexpected error occurred during S3 delete: {e}")
        return False
    return True