# Use a Python base image that fits your needs
FROM python:3.12

# Set the working directory in the container
WORKDIR /app

# Copy the application code into the container
COPY daily_csv_gen.py .

# Install dependencies
# Assuming you have a requirements.txt file in the same directory as your script
COPY requirements.txt .
RUN pip install -r requirements.txt

# Set environment variables.  These are crucial for GCP services.
# ENV GOOGLE_APPLICATION_CREDENTIALS="/app/service_account.json" 
ENV PROJECT_ID="cloud-sql-big-table-data-flow"
ENV INSTANCE_CONNECTION_NAME="cloud-sql-big-table-data-flow:us-central1:fin-serv-instance"
ENV BIGTABLE_INSTANCE_ID="fin-bt"
ENV BIGTABLE_TABLE_ID="txn_tbl"
ENV STORAGE_BUCKET="financial_transactions_daily_summary"

# Specify the command to run when the container starts.  For a job, this is your Python script.
CMD ["python3", "daily_csv_gen.py"] 

# Build Command
# gcloud builds submit --tag gcr.io/cloud-sql-big-table-data-flow/daily_csv_sumaries:latest .

# Job Deploy command -> For When doing code changes  
# gcloud run jobs deploy daily-csv-sumaries \
#   --image gcr.io/cloud-sql-big-table-data-flow/daily_csv_sumaries:latest \
#   --region us-central1