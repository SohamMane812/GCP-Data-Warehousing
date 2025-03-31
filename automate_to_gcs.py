import os
from google.cloud import storage
from dotenv import load_dotenv

# Load environment variables
ENV_PATH = "/home/airflow/gcs/data/.env"
load_dotenv(ENV_PATH)

# Read values from .env
PROJECT_ID = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")
GCS_FILE_PATH = os.getenv("GCS_FILE_PATH")
GCS_FOLDER_NAME = os.getenv("GCS_FOLDER_NAME")
GCS_FILE_NAME = os.getenv("GCS_FILE_NAME")

# Initialize GCS client
storage_client = storage.Client(project=PROJECT_ID)

# Get the source file from GCS
source_bucket_name = GCS_FILE_PATH.split('/')[2]  # Extract bucket name from GCS path
source_file_path = '/'.join(GCS_FILE_PATH.split('/')[3:])  # Extract file path after the bucket name

# Reference to the source bucket and blob
source_bucket = storage_client.bucket(source_bucket_name)
source_blob = source_bucket.blob(source_file_path)

# Download file to a temporary location in Composer (e.g., /tmp)
LOCAL_TMP_PATH = f"/tmp/{GCS_FILE_NAME}"
source_blob.download_to_filename(LOCAL_TMP_PATH)

# Create the destination bucket if it doesn't exist
bucket = storage_client.lookup_bucket(BUCKET_NAME)
if not bucket:
    bucket = storage_client.create_bucket(BUCKET_NAME)
    print(f"Bucket {BUCKET_NAME} created successfully")
else:
    print(f"Bucket {BUCKET_NAME} already exists")

# Upload the file to the destination bucket
gcs_path = f"{GCS_FOLDER_NAME}/{GCS_FILE_NAME}" if GCS_FOLDER_NAME else GCS_FILE_NAME
blob = bucket.blob(gcs_path)
blob.upload_from_filename(LOCAL_TMP_PATH)

print(f"File {LOCAL_TMP_PATH} uploaded to {BUCKET_NAME}/{gcs_path}")
