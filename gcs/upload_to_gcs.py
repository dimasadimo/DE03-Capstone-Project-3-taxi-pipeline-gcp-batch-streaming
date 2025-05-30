from google.cloud import storage
from datetime import date
import os
import json

def convert_json_array_to_ndjson(file_path):
   
    with open(file_path, 'r') as f:
        data = json.load(f)  # Read entire JSON array

    # Convert each object to a JSON string and join with newline
    ndjson_str = "\n".join(json.dumps(record) for record in data)
    return ndjson_str

def upload_to_gcs(bucket_name, source_file_path, file_type):

    today = date.today().isoformat()  # Format: YYYY-MM-DD
    filename = os.path.basename(source_file_path)
    # Authenticate using GOOGLE_APPLICATION_CREDENTIALS env variable

    if file_type == "master":
        destination_blob_name = f"capstone3_dimasadihartomo/master/{filename}"
    else:
        destination_blob_name = f"capstone3_dimasadihartomo/{file_type}/{today}/{filename}"

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    if blob.exists():
        print(f"Skipped: {destination_blob_name} already exists in GCS.")
        return

    if file_type == "json" and filename.endswith(".json"):
        print(f"Converting JSON array to NDJSON for {filename}...")
        ndjson_content = convert_json_array_to_ndjson(source_file_path)
        blob.upload_from_string(ndjson_content, content_type='application/json')
        print(f"Uploaded file to: {destination_blob_name}")
    else:
        # For CSV or other file types, upload file as-is
        blob.upload_from_filename(source_file_path)
        print(f"Uploaded file to: {destination_blob_name}")

def upload_all_files_for_today(bucket_name):
    today = date.today().isoformat()
    base_dir_csv = f"/opt/airflow/source/{today}/csv"
    base_dir_json = f"/opt/airflow/source/{today}/json"
    base_dir_master = "/opt/airflow/source/master"

    for filename in os.listdir(base_dir_csv):
        file_path = os.path.join(base_dir_csv, filename)
        if os.path.isfile(file_path):
            upload_to_gcs(bucket_name, file_path, 'csv')

    for filename in os.listdir(base_dir_json):
        file_path = os.path.join(base_dir_json, filename)
        if os.path.isfile(file_path):
            upload_to_gcs(bucket_name, file_path, 'json')

    for filename in os.listdir(base_dir_master):
        file_path = os.path.join(base_dir_master, filename)
        if os.path.isfile(file_path):
            upload_to_gcs(bucket_name, file_path, 'master')
                
# Example usage (uncomment to run directly)
# Make sure GOOGLE_APPLICATION_CREDENTIALS env var is set
if __name__ == "__main__":
    upload_all_files_for_today(bucket_name="jdeol003-bucket")