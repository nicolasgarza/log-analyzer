import boto3
import os

BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")

LOCAL_FILE_PATH = "data/init_logs.log"

S3_KEY = "logs/logfile.log"


def upload_to_s3(file_path, bucket, key):
    s3 = boto3.client("s3")

    try:
        s3.upload_file(file_path, bucket, key)
        print(f"Successfully uploaded {file_path} to {bucket}/{key}")
    except Exception as e:
        print(f"An error occured: {e}")


if __name__ == "__main__":
    if not os.path.exists(LOCAL_FILE_PATH):
        print(f"File not found: {LOCAL_FILE_PATH}")
    else:
        upload_to_s3(LOCAL_FILE_PATH, BUCKET_NAME, S3_KEY)
