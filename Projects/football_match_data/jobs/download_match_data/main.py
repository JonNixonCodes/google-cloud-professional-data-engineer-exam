# Import libraries
import requests
import tempfile
import datetime
import yaml
from google.cloud import storage

# Load config file
config = None
with open("config.yml", "r") as f:
    try:
        config = yaml.safe_load(f)
    except yaml.YAMLError as err:
        print(err)

# Set up project parameters
project_id = config['PROJECT_ID']
destination_blob_dir = config['DESTINATION_BLOB_DIR']
bucket_name = config['BUCKET_NAME']

# Set up Google Cloud Storage
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)

# Process job function
def process_job(job_config):
    # Set up job parameters
    source_url = job_config["SOURCE_URL"]
    output_file_prefix = job_config["OUTPUT_FILE_PREFIX"]
    output_file_name = output_file_prefix+"_"+datetime.datetime.now().strftime("%Y%m%d%H%M%S")+".csv"
    temp_file_path = tempfile.gettempdir()+"/"+output_file_name
    destination_blob_path = destination_blob_dir+"/"+output_file_name
    # Set up Google Cloud Storage objects
    blob = bucket.blob(destination_blob_path)
    # Download CSV file to temporary location
    r = requests.get(source_url)
    if r.status_code!=200:
        raise Exception(f"GET request failed: {r.status_code}")
    open(temp_file_path, 'wb').write(r.content)
    # Write temporary file to Google Cloud Storage
    blob.upload_from_filename(temp_file_path)

# Main function
def main():
    # Iterate through each job in the config file
    for job_config in config['JOBS']:
        process_job(job_config)

if __name__=="__main__":
    main()
