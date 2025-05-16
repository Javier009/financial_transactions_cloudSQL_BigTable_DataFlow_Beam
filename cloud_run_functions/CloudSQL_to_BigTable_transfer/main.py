import os
import random
import datetime
import string
import pymysql
import mysql.connector
from mysql.connector import errorcode
from flask import Request
from google.cloud import secretmanager
from google.cloud.sql.connector import Connector
from google.cloud import bigtable
from google.cloud.bigtable import row_filters

# BigTable credentials

PROJECT_ID  = "cloud-sql-big-table-data-flow"
INSTANCE_CONNECTION_NAME = "cloud-sql-big-table-data-flow:us-central1:fin-serv-instance"
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

# Fetch secret from Secret Manager
def get_secret(project_id: str, secret_id: str, version: str = "latest") -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8")

# Main connection logic using Cloud SQL Connector
def get_connection():
    db_user = get_secret(PROJECT_ID, CLOUDSQL_SECRET_DB_USER)
    db_pass = get_secret(PROJECT_ID, CLOUDSQL_SECRET_DB_PASS)

    connector = Connector()

    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        driver="pymysql",
        user=db_user,
        password=db_pass,
        db=CLOUDSQL_DB_NAME
    )
    return conn


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
        conn = get_connection()
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

        print(f"✅ Fetched all rows from {cursor.rowcount} `{table}`.")
        return rows_list

    except mysql.connector.Error as err:
        print(f"❌ MySQL error: {err}")

    finally:
        try:
            cursor.close()
        except:
            pass
        try:
            conn.close()
        except:
            pass

def mark_transaction_as_sent(processed_transactions:list):
    try:
        conn = get_connection()
        cursor = conn.cursor()

        placeholders = ','.join([str(i) for i in processed_transactions])

        update_stmt = f"""
            UPDATE transactions
            SET passed_to_big_table = TRUE
            WHERE CAST(transaction_id AS CHAR) IN ({placeholders})
        """
        cursor.execute(update_stmt)
        conn.commit()

        print(f"✅ Processed transactions marked as sent to BigTable.")

    except mysql.connector.Error as err:
        print(f"❌ MySQL error: {err}")

    finally:
        try:
            cursor.close()
        except:
            pass
        try:
            conn.close()
        except:
            pass

def register_records_in_BigTable():
    list_dict=fetch_records('transactions')
    proceesed_transactions = []
    all_rows = []
    if len(list_dict) > 0 :
        try:
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
            print("Inserted demo rows into BigTable")
            # marked transactions as registered in CloudSQL
            mark_transaction_as_sent(proceesed_transactions)
            print(f"✅ Succesfully sent new data to BigTable -> {len(all_rows)} new records")
            return True, len(list_dict)
        except Exception as e:
            print(f"❌ Error inserting data in BigTable: {e}")
            return False, 0
    else:
        print('No new data to be sent')
        return True, 0
    
    

def execute_CloudSQL_to_BigTable(request: Request):
    succesful_transfer_cloudSQL_to_BigTable, records = register_records_in_BigTable()
    if succesful_transfer_cloudSQL_to_BigTable:
        if records >0:
            return f"✅ Succesfully sent new {records} to BigTable -> Review DataFlow streming job, changes shoud have been recorded as CDC ", 200
        else:
            return f"✅  No new records to be sent, all code ran well no worries ", 200
    else:
        return  f"❌ Errors encountered plase review, sent {records} records ", 500
        
