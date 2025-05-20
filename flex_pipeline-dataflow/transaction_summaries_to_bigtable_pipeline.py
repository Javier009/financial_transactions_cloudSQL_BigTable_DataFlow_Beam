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


class TransactionOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--date_suffixes",
            type=str,
            #  default= files_not_yet_processed_default_parameter(generate_dates_captured_in_GCS(), fetch_captured_dates_in_big_table()),
            help="Comma-separated list of date suffixes, e.g. 2025-05-15,2025-05-16"
        )
        parser.add_argument("--input_bucket",  type=str, required=True)
        parser.add_argument("--gcp_project",   type=str, required=True)
        parser.add_argument("--bigtable_instance", type=str, default="fin-bt")
        parser.add_argument("--bigtable_table", type=str, default= "daily_transactions_summaries_from_csv")
        parser.add_argument("--app_profile",   type=str, default="default")

def parse_line(line):
    import csv
    #date, transaction_type, count, sum
    fields = next(csv.reader([line]))
    if fields and fields[0] == 'transaction_date_yyyymmdd': 
        return None
    else:
        return {
            "date": fields[0],
            "transaction_type": fields[1],
            "count": int(fields[2]),
            "sum": float(fields[3])
        }

def to_bt_row(rec):
    from google.cloud.bigtable.row import DirectRow
    if not isinstance(rec, dict):
        print(f"Warning: Received non-dictionary input in to_bt_row: {rec}")
        return None 
    else:
        key = rec['date'].encode("utf-8")
        row = DirectRow(row_key=key)
        transaction_type = rec.get('transaction_type')

        if transaction_type:
            count = rec.get('count')
            sum_value = rec.get('sum')

            if count is not None:
                row.set_cell("count", transaction_type.encode("utf-8"), str(count).encode("utf-8"))
                if count < 30:
                    row.set_cell("count", f"{transaction_type}_lowTransactionValue".encode("utf-8"), b"true")
                else:
                    row.set_cell("count", f"{transaction_type}_lowTransactionValue".encode("utf-8"), b"false")

            if sum_value is not None:
                row.set_cell("sum", transaction_type.encode("utf-8"), str(sum_value).encode("utf-8"))

        return row

def run(pipeline_options):
    
    with beam.Pipeline(options=pipeline_options) as p:
        date_suffixes = pipeline_options.date_suffixes.split(',')
        input_bucket = pipeline_options.input_bucket

        # 1. Create a PCollection of file paths
        file_paths = [f"gs://{input_bucket}/financial_transaction_daily_summary_{date}.csv" for date in date_suffixes]
        # 2. Read the files
        files = p | "CreateFilePatterns" >> beam.Create(file_paths)
        lines = files | "Read from GCS" >> beam.io.ReadAllFromText()
        
        # 3. Parse each line
        records_with_maybe_header = lines | "Parse CSV" >> beam.Map(parse_line)
        records = records_with_maybe_header | "Filter Header" >> beam.Filter(lambda x: x is not None)

        # 4. Convert to Bigtable rows
        bigtable_rows = records | "To Bigtable Row" >> beam.Map(to_bt_row)
        
        # 5. Write to Bigtable
        bigtable_rows | "Write to Bigtable" >> WriteToBigTable(
            project_id=pipeline_options.gcp_project,
            instance_id=pipeline_options.bigtable_instance,
            table_id=pipeline_options.bigtable_table
        )

if __name__ == '__main__':
    options = PipelineOptions().view_as(TransactionOptions)

    if options.date_suffixes == 'ALL_PENDING':
        cloud_dates = generate_dates_captured_in_GCS()
        bt_dates = fetch_captured_dates_in_big_table()
        dates_to_process = files_not_yet_processed_default_parameter(cloud_dates, bt_dates)
        if len(dates_to_process) > 0:
            options.date_suffixes = dates_to_process
            run(options)
        else:
            print('No new dates to process')
    else:
        run(options)

