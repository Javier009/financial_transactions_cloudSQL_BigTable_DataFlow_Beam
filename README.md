# Enhanced Project Documentation: Financial Transactions Analytics Pipeline ([Github Link](YOUR_GITHUB_LINK_HERE))

This document outlines the architecture and implementation of a financial transactions analytics pipeline built on Google Cloud Platform. The system is designed to ingest, process, store, and analyze financial transaction data, culminating in daily summaries and enabling further stream-based analytics.

---

## 1. Core Infrastructure Setup 🏗️

The foundation of the pipeline relies on several key Google Cloud services:

* **Cloud SQL**:
    * A Cloud SQL instance named `fin-serv-instance` was established to host a relational database named `finserv`.
    * This database initially stores raw transaction data.
* **Secret Manager**:
    * Database credentials (`fin_serv_db_user` and `fin_serv_db_password`) are securely stored in Secret Manager.
    * This allows applications to programmatically and securely access the `finserv` database.
* **Cloud Bigtable**:
    * A Bigtable instance named `fin-bt` was created to serve as a scalable NoSQL data store for transaction data replicated from Cloud SQL.
    * Within this instance, a table named `txn_tbl` stores individual transactions, organized with two column families.
    * A second Bigtable table, `daily_transactions_summaries_from_csv`, is used to store aggregated daily summaries derived from CSV files. This table uses `count` and `sum` column families, with the date serving as the row key.

---

## 2. Real-time Transaction Ingestion and Replication 🔄

This section details the flow for generating, ingesting, and replicating transaction data in near real-time.

### Initial Connection Challenges & Resolutions:

An organizational policy (`constraints/sql.restrictAuthorizedNetworks = true`) initially blocked direct connections to the Cloud SQL instance.

* **Resolution 1 (Administrative Access)**:
    * A GCE VM (`cloud-sql-client-vm`) was provisioned in the same VPC/region as the Cloud SQL instance.
    * The Cloud SQL Proxy was installed and run on this VM, enabling secure connections for administrative tasks like table creation and ad-hoc queries.
    * The VM's service account (`run-project-tasks@cloud-sql-big-table-data-flow.iam.gserviceaccount.com`) was granted the `roles/cloudsql.admin` IAM role.
* **Resolution 2 (Application Access)**:
    * Cloud Run services were utilized for programmatic database interactions, bypassing the need for direct authorized networks for the application layer.

### Step 1: Scheduled Data Generation (Cloud Run Function 1) ⚙️

* A Cloud Run function is responsible for generating random financial transaction data.
* This function is HTTP-triggered and scheduled to run hourly, simulating a continuous stream of new transactions.
* The service account executing this Cloud Run function is granted `Cloud SQL Admin` permissions to ingest records directly into the `transactions` table in the Cloud SQL `finserv` database.

### Step 2: Signaling Data Ingestion (Pub/Sub) 📬

* Upon successful ingestion of new data into Cloud SQL, the first Cloud Run function publishes a simple message to a Pub/Sub topic.
* This message acts as a trigger for the next stage in the pipeline.

### Step 3: Data Replication from Cloud SQL to Bigtable (Cloud Run Function 2) ➡️

* A second Cloud Run function is triggered by messages on the aforementioned Pub/Sub topic.
* Its purpose is to move new transaction data from the Cloud SQL `transactions` table to the `txn_tbl` in Cloud Bigtable.
* To prevent reprocessing, this function updates a `sent_to_big_table` boolean column in the Cloud SQL table from `False` to `True` for records successfully transferred. Updates can be observed via the Bigtable Studio UI.

---

## 3. Daily Transaction Summarization (Batch Pipeline) 📊

This pipeline processes the data stored in Bigtable to generate daily summaries and store them back into Bigtable.

### Step 1: Daily Data Extraction and Grouping (Cloud Run Job) 🗓️

* A Python script, deployed as a scheduled Cloud Run Job, reads data from the Bigtable `txn_tbl`.
* It uses Pandas for grouping transactions (with plans to migrate to PySpark) and generates daily summary CSV files named `Financial_transaction_daily_summary_yyyy-mm-dd.csv`.
* The script includes logic to scan Bigtable and only process data for dates that haven't already been summarized and stored in Google Cloud Storage (GCS).
* The job is containerized using a Dockerfile, with the image stored in Google Container Registry (GCR).
* This job is scheduled to run daily at 1:00 AM, allowing time for Bigtable to accumulate the previous day's complete data.

### Step 2: Loading Summaries into Bigtable (Dataflow Flex Template) 🌊

* An Apache Beam pipeline, packaged as a Dataflow Flex Template, processes the CSV files generated in the previous step.
* The pipeline reads the daily summary CSVs from GCS.
* It performs transformations, such as adding a threshold signal based on the number of transactions, and then writes the aggregated data (count and sum) into the `daily_transactions_summaries_from_csv` Bigtable table, using the date as the row key.
* The Flex Template includes parameters (defined in `metadata.json`) like `dates_suffixes` to ensure it only processes new CSV files that haven't been loaded yet.
* Crucially, the logic for determining which date-specific files to process is handled within the Apache Beam Python code itself (using these `dates_suffixes`). This internal logic eliminated the need to create separate, dynamically parameterized Cloud Scheduler jobs for different dates or to pass complex date parameters via the scheduler's launch body JSON.
* This Dataflow job is simply scheduled to run daily at 2:00 AM, and on each run, the code intelligently selects the appropriate new files.

---

## 4. Additional Data Streaming and Schema Enforcement Practices 🧪

Further explorations into data streaming and schema management were conducted:

* **Bigtable Change Data Capture (CDC) with Dataflow**:
    * A Dataflow streaming job (`bit-table-transaction-updates`) was created to capture Change Data Capture (CDC) operations from the Bigtable `txn-tbl`.
    * While implemented, the logs from these CDC operations were not proactively stored.
* **Bigtable to Pub/Sub with AVRO Schema**:
    * To practice schema enforcement, another Pub/Sub topic was set up with an AVRO schema.
    * Logic was implemented to publish records from Bigtable to this topic, adhering to the defined AVRO schema.
    * Similar to other data movement steps, records processed and sent to this Pub/Sub topic were marked in Bigtable (e.g., using a `passed_to_pub_sub` column) to prevent redundant transmissions.