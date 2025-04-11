import requests
import json
import time
import os
from pyspark.sql.types import ArrayType, StructType, StringType, IntegerType, LongType, DoubleType, BooleanType, TimestampType
from google.cloud import bigquery
from google.cloud import secretmanager
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
from datetime import datetime, timedelta
from google.cloud.bigquery import SchemaField


def get_file_name_without_ext(file_path: str) -> str:
    """
    Extract the filename without extension from a file path.
    
    Args:
        file_path: The full path to the file
        
    Returns:
        The filename without extension
    """
    base_name = os.path.basename(file_path)
    name_only = os.path.splitext(base_name)[0]
    return name_only


""" PART 1: GET ALL SECRET VALUE FROM GCP SECRET MANAGER
"""

# Function to access the secret
def access_secret_version(SECRET_NAME, version_id='latest'):
    client = secretmanager.SecretManagerServiceClient()
    name = f"{SECRET_NAME}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    secret_payload = response.payload.data.decode("UTF-8")
    return secret_payload

""" PART 3: LOADING DATA INTO BIGQUERY TABLE
    *independence: 
        final_load_data_into_bigquery   <= load_schema
                                        <= load_data_into_bigquery
                                        <= merge_query <= generate_merge_query
"""
# Load schema and on_codition_columns definitions from the JSON file
def load_schema(schema_file, table_name):
    """ 
        **params:
            schema_file -- json file contains schema of bigquery table
            table_name -- name of table in json file
        **results:
            get data from json file and return
                - schema -- bigquery.schemaField of table
                - on_condition_columns -- columns use to match with current table when loading incremental data
                - schema_columns -- columns in schema, use to pass in generate_merge_query as parameter
    """
    with open(schema_file, 'r') as f:
        schemas = json.load(f)
    
    schema=[bigquery.SchemaField.from_api_repr(field) for field in schemas[table_name]['schema']]
    on_condition_columns = schemas[table_name]["on_condition_columns"]
    schema_columns = [field['name'] for field in schemas[table_name]['schema']]
    return schema, on_condition_columns, schema_columns

# Function to generate the merge query dynamically
def generate_merge_query(target_table_full_id, staging_table_full_id, on_condition_columns, schema_columns):
    """ 
        **results:
            auto generate merge query SQL to update staging table into current table on bigquery base on schema in json file
    """
    # Generate ON condition string
    on_condition_str = " AND ".join([f"T.{col} = S.{col}" for col in on_condition_columns])
    
    # Generate SET clause string for update
    set_clause_str = ",\n".join([f"T.{col} = S.{col}" for col in schema_columns if col not in on_condition_columns])
    
    # Generate INSERT clause string for insert
    insert_columns_str = ", ".join([f"`{item}`" for item in schema_columns])  # fix error column named "in"
    insert_values_str = ", ".join([f"S.{col}" for col in schema_columns])
    
    merge_query = f"""
    MERGE `{target_table_full_id}` T
    USING `{staging_table_full_id}` S
    ON {on_condition_str}
    WHEN MATCHED THEN
        UPDATE SET
            {set_clause_str}
    WHEN NOT MATCHED THEN
        INSERT ({insert_columns_str})
        VALUES ({insert_values_str})
    """
    
    return merge_query

def merge_query(client, target_table_full_id, staging_table_full_id, on_condition_columns, schema_columns):
    """
        **params:
            target_table_full_id -- id of the table needed to load in bigquery (format = `project_id.dataset_id.table_id`)
            staging_table_full_id -- id of the staging table used for storing temporary data in bigquery (format = `project_id.dataset_id.table_id`)
        **results:
            Update existing data and insert new data from staging_table into target_table
    """
    # Merge the staging table with the target table
    merge_query = generate_merge_query(target_table_full_id, staging_table_full_id, on_condition_columns, schema_columns)

    # Execute the merge query with error handling
    client.query(merge_query).result()
    print(f"Data merged into {target_table_full_id} successfully.")

    # Drop the temporary table
    client.delete_table(staging_table_full_id)
    print(f"Temporary table {staging_table_full_id} deleted.")

def load_data_into_bigquery(client, data, table_full_id, schema):
    """ 
        **params:
            data -- data got from API (format = )
            table_full_id -- id of the table needed to load in bigquery (format = `project_id.dataset_id.table_id`)
            schema -- bigquery schema of the table needed to load
        **results:
            Create or replace table in BQ
    """
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    load_job = client.load_table_from_json(data, table_full_id, job_config=job_config)
    load_job.result()  # Wait for the job to complete
    print(f"Loaded {len(data)} rows into {table_full_id}.")

def final_load_data_into_bigquery(client, loading_type, data, dataset_id, table_name, schema_file):
    """ 
        **params:
            loading_type -- 'Update' used for incremental loading new data into current table, it will update matched data and insert new non-matched data 
                            'Create' used for replace all data in current table
            data -- list of data need to load into bigquery table
            dataset_id -- id of dataset in bigquery
            table_name -- name of the table in bigquery
            schema_file -- json file that contained schema of tables in bigquery
        **results:
            Create or replace current table if loading_type=  'Creat'
            Update new data into current table if loading_type=  'Update'
    """
    table_id = f"{client.project}.{dataset_id}.{table_name}"

    # Get schema and on_condition_columns from json file
    schema, on_condition_columns, schema_columns = load_schema(schema_file, table_name)

    if loading_type == 'Create':
        # Load the accumulated data into BigQuery
        if data:
            load_data_into_bigquery(client, data, table_id, schema)
        else:
            print("No data to load")
    elif loading_type == 'Update':
        if data:
            staging_table_id = f'{table_id}_temp'
            # Create staging table
            load_data_into_bigquery(client, data, staging_table_id, schema)

            # Merge staging table into current table
            merge_query(client, table_id, staging_table_id, on_condition_columns, schema_columns)
        else:
            print("No data to load")


def convert_schema(spark_schema):
    bq_schema = []
    for field in spark_schema:
        field_name = field.name
        field_type = field.dataType

        if isinstance(field_type, StructType):
            # Convert STRUCT to RECORD in BigQuery
            bq_schema.append(bigquery.SchemaField(
                field_name, "RECORD", mode="NULLABLE",
                fields=convert_schema(field_type)  # Recursively handle nested fields
            ))

        elif isinstance(field_type, ArrayType):
            element_type = field_type.elementType

            if isinstance(element_type, StructType):
                # Convert ARRAY<STRUCT> to REPEATED RECORD
                bq_schema.append(bigquery.SchemaField(
                    field_name, "RECORD", mode="REPEATED",
                    fields=convert_schema(element_type)  # Recursively convert fields inside STRUCT
                ))

            elif isinstance(element_type, StringType):
                bq_schema.append(bigquery.SchemaField(field_name, "STRING", mode="REPEATED"))
            elif isinstance(element_type, IntegerType):
                bq_schema.append(bigquery.SchemaField(field_name, "INTEGER", mode="REPEATED"))
            elif isinstance(element_type, LongType):
                bq_schema.append(bigquery.SchemaField(field_name, "INTEGER", mode="REPEATED"))
            elif isinstance(element_type, DoubleType):
                bq_schema.append(bigquery.SchemaField(field_name, "FLOAT", mode="REPEATED"))
            elif isinstance(element_type, BooleanType):
                bq_schema.append(bigquery.SchemaField(field_name, "BOOLEAN", mode="REPEATED"))
            elif isinstance(element_type, TimestampType):
                bq_schema.append(bigquery.SchemaField(field_name, "TIMESTAMP", mode="REPEATED"))
            else:
                raise ValueError(f"Unsupported ARRAY type: {element_type}")

        elif isinstance(field_type, StringType):
            bq_schema.append(bigquery.SchemaField(field_name, "STRING", mode="NULLABLE"))
        elif isinstance(field_type, IntegerType):
            bq_schema.append(bigquery.SchemaField(field_name, "INTEGER", mode="NULLABLE"))
        elif isinstance(field_type, LongType):
            bq_schema.append(bigquery.SchemaField(field_name, "INTEGER", mode="NULLABLE"))
        elif isinstance(field_type, DoubleType):
            bq_schema.append(bigquery.SchemaField(field_name, "FLOAT", mode="NULLABLE"))
        elif isinstance(field_type, BooleanType):
            bq_schema.append(bigquery.SchemaField(field_name, "BOOLEAN", mode="NULLABLE"))
        elif isinstance(field_type, TimestampType):
            bq_schema.append(bigquery.SchemaField(field_name, "DATETIME", mode="NULLABLE"))
        else:
            raise ValueError(f"Unsupported data type: {field_type}")

    return bq_schema


def append_bq_schema_to_json(bq_schema, table_name, condition_columns, file_name):
    # Load existing schema.json if it exists, else create an empty dictionary
    if os.path.exists(file_name):
        try:
            with open(file_name, "r") as f:
                schema_json = json.load(f)
        except json.JSONDecodeError:
            print(f"Warning: {file_name} is not a valid JSON file. Creating a new schema.")
            schema_json = {}
    else:
        schema_json = {}

    # Convert bq_schema to the required format
    schema_list = []
    for field in bq_schema:
        field_dict = {
            "name": field.name,
            "type": field.field_type,
            "mode": field.mode
        }

        # Handle nested RECORD fields
        if field.field_type == "RECORD" and field.fields:
            field_dict["fields"] = [
                {"name": subfield.name, "type": subfield.field_type, "mode": subfield.mode}
                for subfield in field.fields
            ]

        schema_list.append(field_dict)

    # Merge with existing schema instead of overwriting
    if table_name in schema_json:
        print(f"Updating existing schema for {table_name} in {file_name}")
    else:
        print(f"Adding new schema for {table_name} to {file_name}")

    schema_json[table_name] = {
        "schema": schema_list,
        "on_condition_columns": condition_columns
    }

    # Write the updated schema back to the file
    try:
        with open(file_name, "w") as f:
            json.dump(schema_json, f, indent=4)
        print(f"Schema successfully written to {file_name}")
    except Exception as e:
        print(f"Error writing to {file_name}: {e}")
