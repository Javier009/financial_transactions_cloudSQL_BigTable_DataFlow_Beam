# Regular imports 
import json
import random
import string
import datetime
import pandas as pd

# Import BigTable Libraries

from google.cloud import bigtable
from google.cloud.bigtable import row_filters
from google.cloud.bigtable.row import DirectRow

# Import Cloud Storage 
from google.cloud import storage

# BigTable credentials

PROJECT_ID  = "cloud-sql-big-table-data-flow"
BIGTABLE_INSTANCE_ID = "fin-bt"
BIGTABLE_TABLE_ID    = "txn_tbl"

bigtable_client   = bigtable.Client(project=PROJECT_ID, admin=True)
bigtable_instance = bigtable_client.instance(BIGTABLE_INSTANCE_ID)
bigtable_table    = bigtable_instance.table(BIGTABLE_TABLE_ID)

# Google Cloud Storage Bucket
STORAGE_BUCKET = 'financial_transactions_daily_summary'

#Current date -> this wont be proceesed until next day when all data is collected
today_str = datetime.datetime.now().strftime("%Y-%m-%d") 
today_date = datetime.datetime.strptime(today_str, "%Y-%m-%d").date()
tomorrow_date = today_date + datetime.timedelta(days=1)
ongoing_reporting_date = tomorrow_date.strftime("%Y-%m-%d")

def tunc_date(date_str:str):
    date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    formatted_date = date_obj.strftime("%Y-%m-%d")
    return formatted_date

def check_or_create_csv_from_df(bucket_name, blob_name, df):

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    if blob.exists():
        return False
    else:
        print(f"File '{blob_name}' does not exist in bucket '{bucket_name}'. Creating it.")
        try:
            # Create a CSV string from the DataFrame
            csv_string = df.to_csv(index=False, header=True)
            # Upload the CSV string to GCS
            blob.upload_from_string(csv_string, content_type='text/csv')
            print(f"File '{blob_name}' successfully created in bucket '{bucket_name}'.")
            return True
        except Exception as e:
            print(f"Error creating file '{blob_name}': {e}")
            return False 

def generate_daily_summaries_form_big_table():
    try:
        big_table_data_list = []
        print("\n--- Scan all rows ---")
        partial = bigtable_table.read_rows()
        partial.consume_all()
        for row in partial.rows.values():
            data = {
                f"{cf}:{col.decode()}": cell[0].value.decode()
                for cf, cols in row.cells.items()
                for col, cell in cols.items()
                }
            # Trunc data with yyyy-mm--dd format
            data['transaction_date_yyyymmdd'] = tunc_date(data['cf_history:transaction_date'])
            # Convert ammount to float 
            data['cf_latest:amount'] = float(data['cf_latest:amount'])
            big_table_data_list.append(data)
        
        # Won't consider the current date as more data is comming and once the day has ended will be passed
        unique_dates = set([d['transaction_date_yyyymmdd'] for d in big_table_data_list if d['transaction_date_yyyymmdd'] not in ongoing_reporting_date])
        transactions_df = pd.DataFrame(big_table_data_list)
        grouped_df = transactions_df.groupby(['transaction_date_yyyymmdd', 'cf_latest:transaction_type']).\
                    agg(number_of_transactions=('cf_latest:transaction_type', 'count'),total_ammount=('cf_latest:amount', 'sum')).\
                    reset_index()
        
        # Send to GCS
        files_created = 0 
        for date in unique_dates:
            iterating_date_df = grouped_df[grouped_df['transaction_date_yyyymmdd'] == date].reset_index(drop=True)
            if check_or_create_csv_from_df(bucket_name=STORAGE_BUCKET,
                                        blob_name=f'financial_transaction_daily_summary_{date}.csv',
                                        df=iterating_date_df):
               files_created += 1
        
        if files_created == 0:
            print('No new files added. All data already captured in GCS')
        else:
            print(f'Created {files_created} new file/s')
             
    except Exception as e:
        raise (f'Error fetching data from big table {e}')
        
if __name__ == '__main__':
    generate_daily_summaries_form_big_table()