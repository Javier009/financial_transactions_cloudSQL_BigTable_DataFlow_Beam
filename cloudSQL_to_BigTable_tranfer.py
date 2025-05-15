import random
import string
from collections import defaultdict
from datetime import datetime

from google.cloud import secretmanager
import mysql.connector
from mysql.connector import errorcode
from google.cloud import bigtable
from google.cloud.bigtable import row_filters

# BigTable credentials

PROJECT_ID  = "cloud-sql-big-table-data-flow"
BIGTABLE_INSTANCE_ID = "fin-bt"
BIGTABLE_TABLE_ID    = "txn_tbl"

bigtable_client   = bigtable.Client(project=PROJECT_ID, admin=True)
bigtable_instance = bigtable_client.instance(BIGTABLE_INSTANCE_ID)
bigtable_table    = bigtable_instance.table(BIGTABLE_TABLE_ID)


# CloudSQL credentials
CLOUDSQL_INSTANCE  = "cloud-sql-big-table-data-flow:us-central1:fin-serv-instance"  
# CLOUDSQL_DB_HOST         = "34.42.81.55"
CLOUDSQL_DB_HOST  =  "127.0.0.1"
CLOUDSQL_DB_PORT  = 3306
CLOUDSQL_DB_NAME  = 'finserv'

# Secret names in Secret Manager
CLOUDSQL_SECRET_DB_USER = "fin_serv_db_user"
CLOUDSQL_SECRET_DB_PASS = "fin_serv_db_password"

def get_secret(project_id: str, secret_id: str, version: str = "latest") -> str:
    """
    Fetch the payload of the given secret version from Secret Manager.
    """
    client = secretmanager.SecretManagerServiceClient()
    name   = f"projects/{project_id}/secrets/{secret_id}/versions/{version}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


try:
    db_user = get_secret(PROJECT_ID, CLOUDSQL_SECRET_DB_USER)
    db_pass = get_secret(PROJECT_ID, CLOUDSQL_SECRET_DB_PASS)
    print(f'Succesfully fetched credentials from Secret Manager: {db_user}, {db_pass}')
except Exception as e:
    print(f"Failed to fetch secrets: {e}")


def generate_unique_id():
    letters = ''.join(random.choices(string.ascii_uppercase, k=5))
    numbers = ''.join(random.choices(string.digits, k=9))
    return letters + numbers

# Create connection to CLoudSQL database and fetch data from the table
def fetch_records(table):
    """
    Returns a list of dictionaries with all the records from a CloudSQL table
    """
    try:
        rows_list = []
        conn = mysql.connector.connect(
            user=db_user,
            password=db_pass,
            host=CLOUDSQL_DB_HOST,
            port=CLOUDSQL_DB_PORT,
            database=CLOUDSQL_DB_NAME or None
        )
        cursor = conn.cursor()

        sql_stmt = f"""
            SELECT * FROM {table} WHERE passed_to_big_table = FALSE
        """
        cursor.execute(sql_stmt)
        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        for row in rows:
            row_dict = dict(zip(column_names, row))
            rows_list.append(row_dict)
            #print(row_dict)
        print(f"✅ Fetched all rows from {cursor.rowcount} `{table}`.")
        return rows_list

    except mysql.connector.Error as err:
        print(f"❌ MySQL error: {err}")

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def mark_transaction_as_sent(processed_transactions:list):
    try:
        conn = mysql.connector.connect(
            user=db_user,
            password=db_pass,
            host=CLOUDSQL_DB_HOST,
            port=CLOUDSQL_DB_PORT,
            database=CLOUDSQL_DB_NAME or None
        )
        cursor = conn.cursor()

        placeholders = ','.join([str(i) for i in processed_transactions])

        update_stmt = f"""
            UPDATE transactions
            SET passed_to_big_table = TRUE
            WHERE CAST(transaction_id AS CHAR) IN ({placeholders})
        """

        cursor.execute(update_stmt)
        conn.commit()

        # print(f"✅ Transaction {transaction_id} marked as sent to BigTable.")

    except mysql.connector.Error as err:
        print(f"❌ MySQL error: {err}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def register_records_in_BigTable(list_dict=fetch_records('transactions')):
    proceesed_transactions = []
    all_rows = []
    for row in list_dict:
        row_id = generate_unique_id()
        transaction_id = row['transaction_id']
        account_id = row['account_id']
        amount = row['amount']
        currency = row['currency']
        transaction_type = row['transaction_type']
        transaction_date = row['transaction_date']
        description = row['description']
        status = row['status']
        created_at = row['created_at']
        updated_at = row['updated_at']

        record_tupple = (row_id, {
                                 "cf_latest:transaction_id":str(transaction_id),
                                 "cf_latest:account_id":str(account_id),
                                 "cf_latest:amount":str(amount),
                                 "cf_latest:currency":str(currency),
                                 "cf_latest:transaction_type":str(transaction_type),
                                 "cf_latest:passed_to_pubsub":'False',
                                 "cf_history:transaction_date":str(transaction_date),
                                 "cf_history:description":str(description),
                                 "cf_history:status":str(status),
                                 "cf_history:created_at":str(created_at),
                                 "cf_history:updated_at":str(updated_at)
                                 }
                        )
        
        all_rows.append(record_tupple)
        proceesed_transactions.append(transaction_id)
    
    for key, data in all_rows:
        r = bigtable_table.direct_row(key)
        for colfam_col, val in data.items():
            cf, col = colfam_col.split(":", 1)
            r.set_cell(cf, col, val)
        r.commit()
    print("Inserted demo rows")
    # marked transactions as registered in CloudSQL
    mark_transaction_as_sent(proceesed_transactions)
                                 

if __name__ == '__main__':
    register_records_in_BigTable()