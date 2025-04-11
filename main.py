import os
import json
import glob
import time
import asyncio
import concurrent.futures
import csv
import tempfile
import shutil
import re
from google.cloud import storage, bigquery
from pyspark.sql import SparkSession
from gcp_function import convert_schema, append_bq_schema_to_json, get_file_name_without_ext

# Set up GCP credentials and project ID
PROJECT_ID = 'vu-kim'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'KEY'

storage_client = storage.Client(project=PROJECT_ID)

bq_client = bigquery.Client(project=PROJECT_ID)

# Define constants
BUCKET_NAME = "gmt-data-bucket"
YELP_DATASET_ID = "STG_Yelp"
CLIMATE_DATASET_ID = "STG_Climate"
DATA_FOLDER = "Data_AE_Test"
SCHEMA_FILE = "schema.json"

# Create GCS buckets if they don't exist
try:
    bucket = storage_client.get_bucket(BUCKET_NAME)
    print(f"Bucket {BUCKET_NAME} already exists")
except Exception:
    bucket = storage_client.create_bucket(BUCKET_NAME)
    print(f"Bucket {BUCKET_NAME} created successfully")

# Create STG_Yelp and STG_Climate datasets if they don't exist
try:
    bq_client.get_dataset(YELP_DATASET_ID)
    print(f"Dataset {YELP_DATASET_ID} already exists")
except Exception:
    dataset = bigquery.Dataset(f"{PROJECT_ID}.{YELP_DATASET_ID}")
    dataset.location = "US"  
    dataset = bq_client.create_dataset(dataset, exists_ok=True)
    print(f"Dataset {YELP_DATASET_ID} created successfully")

try:
    bq_client.get_dataset(CLIMATE_DATASET_ID)
    print(f"Dataset {CLIMATE_DATASET_ID} already exists")
except Exception:
    dataset = bigquery.Dataset(f"{PROJECT_ID}.{CLIMATE_DATASET_ID}")
    dataset.location = "US"  
    dataset = bq_client.create_dataset(dataset, exists_ok=True)
    print(f"Dataset {CLIMATE_DATASET_ID} created successfully")

# Function to find all JSON files in the folder recursively
def find_json_files(base_folder):
    json_files = []
    for root, dirs, files in os.walk(base_folder):
        for file in files:
            if file.endswith('.json'):
                json_files.append(os.path.join(root, file))
    return json_files

# Function to find all CSV files in the folder recursively
def find_csv_files(base_folder):
    csv_files = []
    for root, dirs, files in os.walk(base_folder):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))
    return csv_files

# Get all JSON files
json_files = find_json_files(DATA_FOLDER)
if json_files:
    print(f"Found {len(json_files)} JSON files:")
    for file in json_files:
        print(f"  - {file}")
else:
    print(f"No JSON files found in {DATA_FOLDER}. Please check the directory.")

# Get all CSV files
csv_files = find_csv_files(DATA_FOLDER)
if csv_files:
    print(f"Found {len(csv_files)} CSV files:")
    for file in csv_files[:5]:  # Show first 5 files
        print(f"  - {file}")
    if len(csv_files) > 5:
        print(f"  ... and {len(csv_files) - 5} more")
else:
    print(f"No CSV files found in {DATA_FOLDER}. Please check the directory.")
    

def upload_to_gcs_sync(file_path, destination_folder="yelp_data", max_retries=3, retry_delay=5, target_bucket=None, file_format="JSON"):
    # Create a temporary directory for sanitized files
    temp_dir = tempfile.mkdtemp()
    sanitized_file = file_path
    
    try:
        # Sanitize the file based on its format
        if file_format.upper() == "JSON":
            sanitized_file = sanitize_json_field_names(file_path, temp_dir)
        elif file_format.upper() == "CSV":
            sanitized_file = sanitize_csv_headers(file_path, temp_dir)
        
        destination_blob_name = f"{destination_folder}/{os.path.basename(file_path)}"
        
        target_bucket = target_bucket or bucket
        bucket_name = target_bucket.name
        
        for attempt in range(max_retries):
            try:
                blob = target_bucket.blob(destination_blob_name)
                blob.upload_from_filename(sanitized_file, timeout=1200)  
                
                gcs_uri = f"gs://{bucket_name}/{destination_blob_name}"
                print(f"File {file_path} uploaded to {gcs_uri}")
                return file_path, gcs_uri
            except TimeoutError as e:
                if attempt < max_retries - 1:
                    print(f"Timeout uploading {file_path}, retrying in {retry_delay} seconds... (Attempt {attempt+1}/{max_retries})")
                    time.sleep(retry_delay)
                    # Increase delay for next retry (exponential backoff)
                    retry_delay *= 2
                else:
                    print(f"Failed to upload {file_path} after {max_retries} attempts: {str(e)}")
                    raise
            except Exception as e:
                print(f"Error uploading {file_path}: {str(e)}")
                if attempt < max_retries - 1:
                    print(f"Retrying in {retry_delay} seconds... (Attempt {attempt+1}/{max_retries})")
                    time.sleep(retry_delay)
                    # Increase delay for next retry (exponential backoff)
                    retry_delay *= 2
                else:
                    print(f"Failed to upload {file_path} after {max_retries} attempts: {str(e)}")
                    raise
    finally:
        # Clean up temporary files
        if sanitized_file != file_path and os.path.exists(sanitized_file):
            try:
                os.remove(sanitized_file)
            except:
                pass
        
        # Remove temporary directory
        try:
            shutil.rmtree(temp_dir)
        except:
            pass

# Async wrapper for the upload function
async def upload_to_gcs_async(file_path, destination_folder="yelp_data", target_bucket=None, file_format="JSON"):
    loop = asyncio.get_event_loop()
    
    upload_func = lambda: upload_to_gcs_sync(
        file_path=file_path, 
        destination_folder=destination_folder,
        target_bucket=target_bucket,
        file_format=file_format
    )
    
    return await loop.run_in_executor(None, upload_func)

# Function to upload all files concurrently
async def upload_files_concurrently(file_paths, destination_folder="yelp_data", max_concurrency=6, target_bucket=None, file_format="JSON"):
    semaphore = asyncio.Semaphore(max_concurrency)
    
    async def upload_with_semaphore(file_path):
        async with semaphore:
            try:
                return await upload_to_gcs_async(file_path, destination_folder, target_bucket, file_format)
            except Exception as e:
                print(f"Error in async upload of {file_path}: {str(e)}")

                return file_path, None
    
    tasks = [upload_with_semaphore(file_path) for file_path in file_paths]
    
    results = await asyncio.gather(*tasks, return_exceptions=False)
    
    successful_uploads = [result for result in results if result[1] is not None]
    failed_count = len(results) - len(successful_uploads)
    
    if failed_count > 0:
        print(f"Warning: {failed_count} files failed to upload")
    
    return successful_uploads

# Function to sanitize field names in JSON files
def sanitize_json_field_names(file_path, temp_dir=None):

    try:
        # Read the original file
        with open(file_path, 'r', encoding='utf-8') as f:
            # Check if it's a newline-delimited JSON
            first_line = f.readline().strip()
            f.seek(0)  # Reset file pointer
            
            try:
                # Try to parse the first line as JSON
                first_obj = json.loads(first_line)
                is_newline_delimited = True
            except json.JSONDecodeError:
                # If it fails, try to parse the whole file as a single JSON object or array
                try:
                    content = f.read()
                    json_data = json.loads(content)
                    is_newline_delimited = False
                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON file {file_path}: {str(e)}")
                    return file_path  
        
        # Function to sanitize keys in a dictionary
        def sanitize_keys(obj):
            if isinstance(obj, dict):
                new_obj = {}
                for key, value in obj.items():
                    # Replace spaces and special characters with underscores
                    new_key = re.sub(r'[^a-zA-Z0-9_]', '_', key)
                    # Ensure it starts with a letter or underscore
                    if new_key and not (new_key[0].isalpha() or new_key[0] == '_'):
                        new_key = f"_{new_key}"
                    # Recursively sanitize nested objects
                    new_obj[new_key] = sanitize_keys(value)
                return new_obj
            elif isinstance(obj, list):
                return [sanitize_keys(item) for item in obj]
            else:
                return obj
        
        # Create a temporary file for the sanitized output
        if temp_dir is None:
            temp_dir = os.path.dirname(file_path)
        
        temp_file = os.path.join(temp_dir, f"sanitized_{os.path.basename(file_path)}")
        
        # Process the file based on its format
        if is_newline_delimited:
            with open(file_path, 'r', encoding='utf-8') as f_in, open(temp_file, 'w', encoding='utf-8') as f_out:
                for line in f_in:
                    if line.strip():  # Skip empty lines
                        obj = json.loads(line)
                        sanitized_obj = sanitize_keys(obj)
                        f_out.write(json.dumps(sanitized_obj) + '\n')
        else:
            with open(temp_file, 'w', encoding='utf-8') as f_out:
                sanitized_data = sanitize_keys(json_data)
                if isinstance(json_data, list):
                    # If it's an array, write each item as a separate line for BigQuery compatibility
                    for item in sanitized_data:
                        f_out.write(json.dumps(item) + '\n')
                else:
                    # If it's a single object, write it as is
                    f_out.write(json.dumps(sanitized_data))
        
        print(f"Sanitized JSON field names in {file_path} -> {temp_file}")
        return temp_file
    
    except Exception as e:
        print(f"Error sanitizing JSON file {file_path}: {str(e)}")
        return file_path  

# Function to sanitize CSV headers
def sanitize_csv_headers(file_path, temp_dir=None):

    try:
        # Create a temporary file for the sanitized output
        if temp_dir is None:
            temp_dir = os.path.dirname(file_path)
        
        temp_file = os.path.join(temp_dir, f"sanitized_{os.path.basename(file_path)}")
        
        with open(file_path, 'r', newline='', encoding='utf-8') as f_in, open(temp_file, 'w', newline='', encoding='utf-8') as f_out:
            reader = csv.reader(f_in)
            writer = csv.writer(f_out)
            
            # Read the header row
            try:
                header = next(reader)
                
                # Sanitize header names
                sanitized_header = []
                for col in header:
                    # Trim whitespace and replace spaces/special chars with underscores
                    col = col.strip()
                    sanitized_col = re.sub(r'[^a-zA-Z0-9_]', '_', col)
                    
                    # Ensure it starts with a letter or underscore
                    if sanitized_col and not (sanitized_col[0].isalpha() or sanitized_col[0] == '_'):
                        sanitized_col = f"_{sanitized_col}"
                    
                    sanitized_header.append(sanitized_col)
                
                # Write the sanitized header
                writer.writerow(sanitized_header)
                
                # Copy the rest of the file as is
                for row in reader:
                    writer.writerow(row)
                    
                print(f"Sanitized CSV headers in {file_path} -> {temp_file}")
                return temp_file
                
            except StopIteration:
                # Empty file, just return the original
                return file_path
                
    except Exception as e:
        print(f"Error sanitizing CSV file {file_path}: {str(e)}")
        return file_path  

# Function to create an external table in BigQuery from a GCS file
def create_external_table(gcs_uri, table_name, dataset_id, file_format="JSON", schema=None):

    try:
        # Define the external config based on file format
        if file_format.upper() == "JSON":
            external_config = bigquery.ExternalConfig("NEWLINE_DELIMITED_JSON")
            external_config.autodetect = True
        elif file_format.upper() == "CSV":
            external_config = bigquery.ExternalConfig("CSV")
            external_config.autodetect = True
            external_config.skip_leading_rows = 1  
            external_config.csv_options.allow_quoted_newlines = True
            external_config.csv_options.allow_jagged_rows = True
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
        
        # Set the source URI
        external_config.source_uris = [gcs_uri]
        
        if schema:
            external_config.autodetect = False
            external_config.schema = schema
        
        # Define the table
        table_id = f"{PROJECT_ID}.{dataset_id}.{table_name}"
        table = bigquery.Table(table_id, schema=schema)
        table.external_data_configuration = external_config
        
        # Create the table
        table = bq_client.create_table(table, exists_ok=True)
        print(f"Created external table {table_id} pointing to {gcs_uri}")
        return table
    
    except Exception as e:
        print(f"Error creating external table for {gcs_uri}: {str(e)}")
        return None

# Function to handle concurrent uploads with error handling and fallback
async def process_uploads(files, destination_folder, target_bucket=None, bucket_name_display="", 
                          create_tables=False, dataset_id=None, file_format="JSON"):
    if not files:
        print(f"No files to upload to {bucket_name_display or 'GCS'}.")
        return []
    
    print(f"Starting concurrent upload of {len(files)} files to {bucket_name_display or 'GCS'}...")
    
    try:
        results = await upload_files_concurrently(
            files, 
            destination_folder, 
            target_bucket=target_bucket,
            file_format=file_format
        )
        
        # Report results
        if results:
            print(f"Successfully uploaded {len(results)} out of {len(files)} files to {bucket_name_display or 'GCS'}")
            if len(results) < len(files):
                print(f"Warning: {len(files) - len(results)} files failed to upload")
                
            # Create external tables if requested
            if create_tables and dataset_id:
                print(f"Creating external tables in dataset {dataset_id}...")
                tables_created = 0
                
                for file_path, gcs_uri in results:
                    if gcs_uri:
                        # Get the file name without extension to use as table name
                        file_name = get_file_name_without_ext(file_path)
                        # Create a valid BigQuery table name (remove invalid characters)
                        table_name = ''.join(c if c.isalnum() else '_' for c in file_name)
                        # Ensure table name starts with a letter or underscore
                        if not table_name[0].isalpha() and table_name[0] != '_':
                            table_name = f"t_{table_name}"
                            
                        # Create the external table
                        table = create_external_table(gcs_uri, table_name, dataset_id, file_format)
                        if table:
                            tables_created += 1
                
                print(f"Created {tables_created} external tables in dataset {dataset_id}")
        else:
            print(f"No files were successfully uploaded to {bucket_name_display or 'GCS'}")
        
        return results
            
    except Exception as e:
        print(f"Error during concurrent upload to {bucket_name_display or 'GCS'}: {str(e)}")
        print("Falling back to sequential upload...")
        
        results = []
        for file_path in files:
            try:
                result = upload_to_gcs_sync(
                    file_path, 
                    destination_folder, 
                    target_bucket=target_bucket,
                    file_format=file_format
                )
                results.append(result)
                print(f"Sequential upload: {len(results)}/{len(files)} completed")
                
                # Create external table for this file if requested
                if create_tables and dataset_id and result and result[1]:
                    file_name = get_file_name_without_ext(file_path)
                    table_name = ''.join(c if c.isalnum() else '_' for c in file_name)
                    if not table_name[0].isalpha() and table_name[0] != '_':
                        table_name = f"t_{table_name}"
                    create_external_table(result[1], table_name, dataset_id, file_format)
                    
            except Exception as upload_error:
                print(f"Failed to upload {file_path}: {str(upload_error)}")
        
        return results

async def main():
    # Upload JSON files to the data bucket and create external tables in STG_Yelp
    json_results = await process_uploads(
        files=json_files, 
        destination_folder="yelp_data", 
        target_bucket=bucket, 
        bucket_name_display=f"bucket '{BUCKET_NAME}'",
        create_tables=True,
        dataset_id=YELP_DATASET_ID,
        file_format="JSON"
    )
    
    # Upload CSV files to the data bucket and create external tables in STG_Climate
    csv_results = await process_uploads(
        files=csv_files, 
        destination_folder="climate_data", 
        target_bucket=bucket, 
        bucket_name_display=f"bucket '{BUCKET_NAME}'",
        create_tables=True,
        dataset_id=CLIMATE_DATASET_ID,
        file_format="CSV"
    )
    
    return json_results, csv_results

# Run the main function
if __name__ == "__main__":
    try:

        json_uris, csv_uris = asyncio.run(main())

        print("\nUpload Summary:")
        print(f"- JSON files uploaded to {BUCKET_NAME}: {len(json_uris) if json_uris else 0}")
        print(f"- CSV files uploaded to {BUCKET_NAME}: {len(csv_uris) if csv_uris else 0}")
        
        print("\nExternal Tables Created:")
        print(f"- Tables in {YELP_DATASET_ID} dataset: {len(json_uris) if json_uris else 0}")
        print(f"- Tables in {CLIMATE_DATASET_ID} dataset: {len(csv_uris) if csv_uris else 0}")
        
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
    