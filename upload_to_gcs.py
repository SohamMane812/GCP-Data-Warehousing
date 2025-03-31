import os
from google.cloud import storage
from dotenv import load_dotenv
import json

# Load environment variables from .env file
load_dotenv()

# Read values from .env
PROJECT_ID = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")
LOCAL_FILE_PATH = os.getenv("LOCAL_FILE_PATH") 
GCS_FOLDER_NAME = os.getenv("GCS_FOLDER_NAME")
GCS_FILE_NAME = os.getenv("GCS_FILE_NAME")
CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")

# Set up credentials
if CREDENTIALS_JSON:
    try:
        # Set the credentials directly in the environment variable
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_JSON
    except json.JSONDecodeError:
        print("Error: GOOGLE_CREDENTIALS_JSON is not valid JSON")
        exit(1)
else:
    print("Error: GOOGLE_CREDENTIALS_JSON not set in .env file")
    exit(1)

# Initialize GCS client
try:
    storage_client = storage.Client(project=PROJECT_ID)
except Exception as e:
    print(f"Error initializing storage client: {str(e)}")
    exit(1)

# Create bucket if it doesn't exist
try:
    bucket = storage_client.bucket(BUCKET_NAME)
    if not bucket.exists():
        bucket = storage_client.create_bucket(BUCKET_NAME)
        print(f"Bucket {BUCKET_NAME} created successfully")
    else:
        print(f"Bucket {BUCKET_NAME} already exists")
except Exception as e:
    print(f"Error accessing bucket: {str(e)}")
    exit(1)

# Upload file to bucket in specified folder
try:
    gcs_path = f"{GCS_FOLDER_NAME}/{GCS_FILE_NAME}" if GCS_FOLDER_NAME else GCS_FILE_NAME
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(LOCAL_FILE_PATH)
    print(f"File {LOCAL_FILE_PATH} uploaded to {BUCKET_NAME}/{gcs_path}")
except Exception as e:
    print(f"Error uploading file: {str(e)}")
    exit(1)