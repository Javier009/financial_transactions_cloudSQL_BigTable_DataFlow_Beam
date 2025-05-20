import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io.gcp.bigtableio import WriteToBigTable
from google.cloud.bigtable.row import DirectRow
import csv
import re 

from google.cloud import storage
from google.cloud import bigtable
# from google.cloud.bigtable import row_filters

# Flow
# 1 Extract files from Cloud Storage and fetch dates captured and put them in a list
# 2 Fetch dates captured in big table, These dates will be the row key
# 3 Generate a list of dates that are not yet Captured in the Big Table and use it as parameter in Apache Beam Application
# 4 Build Appache Beam application and deploy in GCP with the correspoinding parameters

PROJECT_ID  = "cloud-sql-big-table-data-flow"

# Google Cloud Storage Bucket

STORAGE_BUCKET = 'financial_transactions_daily_summary'

# BigTable credentials

BIGTABLE_INSTANCE_ID = "fin-bt"
BIGTABLE_TABLE_ID    = "daily_transactions_summaries_from_csv"

bigtable_client   = bigtable.Client(project=PROJECT_ID, admin=True)
bigtable_instance = bigtable_client.instance(BIGTABLE_INSTANCE_ID)
bigtable_table    = bigtable_instance.table(BIGTABLE_TABLE_ID)


def generate_dates_captured_in_GCS():
    """Retrives dates captured in GCS"""
    import re
    storage_client = storage.Client()
    bucket = storage_client.bucket(STORAGE_BUCKET)
    blobs = bucket.list_blobs()
    captured_dates = []
    for blob in blobs:
        blob_str = str(blob)
        date_match = re.search(r'\d{4}-\d{2}-\d{2}', blob_str)
        if date_match:
            date_string = date_match.group(0)
            captured_dates.append(date_string)
        else:
            print(f"No date parameter found in {blob} file, skipping")
    return captured_dates
        

def fetch_captured_dates_in_big_table():
    partial = bigtable_table.read_rows()
    partial.consume_all()
    bigtable_captured_dates = []
    for byte_date in partial.rows.keys():
        date = byte_date.decode()
        bigtable_captured_dates.append(date)
    return bigtable_captured_dates

def files_not_yet_processed_default_parameter(cloud_storage_dates, bigtable_dates):
    pending_big_table_dates = ','.join([d for d in cloud_storage_dates if d not in bigtable_dates])
    print(pending_big_table_dates)
    return pending_big_table_dates

d= files_not_yet_processed_default_parameter(generate_dates_captured_in_GCS(), fetch_captured_dates_in_big_table())
print(len(d))