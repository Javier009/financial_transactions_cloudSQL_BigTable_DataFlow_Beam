import os
import random
import datetime
import string
import pymysql

from flask import Request
from google.cloud import secretmanager
from google.cloud.sql.connector import Connector
import mysql.connector
from mysql.connector import errorcode



# Project and instance info
PROJECT_ID = "cloud-sql-big-table-data-flow"
INSTANCE_CONNECTION_NAME = "cloud-sql-big-table-data-flow:us-central1:fin-serv-instance"
DB_NAME = "finserv"

# Secret names in Secret Manager
SECRET_DB_USER = "fin_serv_db_user"
SECRET_DB_PASS = "fin_serv_db_password"

# Fetch secret from Secret Manager
def get_secret(project_id: str, secret_id: str, version: str = "latest") -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8")

# Main connection logic using Cloud SQL Connector
def get_connection():
    db_user = get_secret(PROJECT_ID, SECRET_DB_USER)
    db_pass = get_secret(PROJECT_ID, SECRET_DB_PASS)

    connector = Connector()

    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        driver="pymysql",
        user=db_user,
        password=db_pass,
        db=DB_NAME
    )
    return conn

# Flask-compatible test endpoint
def cloud_sql_data_generation(table):
    number_of_records = random.randint(50,100)
    try:
        conn = get_connection()
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
        return True, {cursor.rowcount}
    
    except Exception as e:
        print(f"❌ Connection error: {e}")
        return False, {cursor.rowcount}
    
    finally:
        try:
            cursor.close()
        except:
            pass
        try:
            conn.close()
        except:
            pass
    
    
def execute_request(request: Request):
    sucussfull_data_ingestion, rows_added = cloud_sql_data_generation(table='transactions')
    if sucussfull_data_ingestion:
        return f'✅ Data ingestion was succesful with {rows_added} new rows', 200
    else:
        return  f"❌ Errors encountered plase review, {rows_added} rows", 500

    

       