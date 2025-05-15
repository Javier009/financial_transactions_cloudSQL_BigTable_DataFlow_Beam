import os
import io
import random
import datetime
import string
from google.cloud import secretmanager
import mysql.connector
from mysql.connector import errorcode

PROJECT_ID      = "cloud-sql-big-table-data-flow"
INSTANCE        = "cloud-sql-big-table-data-flow:us-central1:fin-serv-instance"  
# DB_HOST         = "34.42.81.55"
DB_HOST         =  "127.0.0.1"
DB_PORT         = 3306
DB_NAME         = 'finserv'

# Secret names in Secret Manager
SECRET_DB_USER = "fin_serv_db_user"
SECRET_DB_PASS = "fin_serv_db_password"


def get_secret(project_id: str, secret_id: str, version: str = "latest") -> str:
    """
    Fetch the payload of the given secret version from Secret Manager.
    """
    client = secretmanager.SecretManagerServiceClient()
    name   = f"projects/{project_id}/secrets/{secret_id}/versions/{version}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


try:
    db_user = get_secret(PROJECT_ID, SECRET_DB_USER)
    db_pass = get_secret(PROJECT_ID, SECRET_DB_PASS)
    print(f'Succesfully fetched credentials from Secret Manager: {db_user}, {db_pass}')
except Exception as e:
    print(f"Failed to fetch secrets: {e}")

# Test the connection
# try:
#     conn = mysql.connector.connect(
#         user=db_user,
#         password=db_pass,
#         host=DB_HOST,
#         port=DB_PORT,
#         database=DB_NAME or None
#         )
#     print(f"✅ Connected to {DB_HOST}:{DB_PORT} as {db_user}")

# except mysql.connector.Error as err:
#      if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
#         print("Authentication error: check your secrets")
#      else:
#          print(f"Connection error: {err}")


def generate_records(number_of_records, table):
    try:
        conn = mysql.connector.connect(
            user=db_user,
            password=db_pass,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME or None
        )
        cursor = conn.cursor()

        TRANSACTION_TYPES = ['deposit', 'withdrawal', 'transfer', 'payment']
        STATUSES = ['completed', 'pending', 'failed']

        insert_stmt = f"""
            INSERT INTO {table} (
                account_id, amount, currency, transaction_type,
                transaction_date, description, status, passed_to_big_table
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        transactions = []
        for _ in range(number_of_records):
            account_id = random.randint(1000, 9999)
            amount = round(random.uniform(10, 5000), 2) * random.choice([1, -1])
            currency = 'USD'
            tx_type = random.choice(TRANSACTION_TYPES)
            tx_date = datetime.datetime.now()
            description = 'A simple transaction'
            status = random.choice(STATUSES)
            passed_to_BigTable = False 

            transactions.append((account_id, amount, currency, tx_type, tx_date, description, status, passed_to_BigTable))

        cursor.executemany(insert_stmt, transactions)
        conn.commit()

        print(f"✅ Inserted {cursor.rowcount} fake transactions into `{table}`.")

    except mysql.connector.Error as err:
        print(f"❌ MySQL error: {err}")

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()


if __name__ == '__main__':
    generate_records(100, 'transactions')