# Use Google’s Python39 Flex-Template launcher base
FROM gcr.io/dataflow-templates-base/python39-template-launcher-base:latest

WORKDIR /templates

# 1) Install all your deps at build time
COPY requirements.txt .
RUN pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# 2) Copy your pipeline code
COPY transaction_summaries_to_bigtable_pipeline.py . 

# 3) Tell the launcher which script to run
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/templates/transaction_summaries_to_bigtable_pipeline.py"

# 4) Crucial: tell Dataflow to use your image as the SDK container
ENV FLEX_TEMPLATE_PYTHON_CONTAINER_IMAGE="gcr.io/cloud-sql-big-table-data-flow/daily_transactions_summaries:latest"

# Deploy commands

# gcloud builds submit --tag gcr.io/cloud-sql-big-table-data-flow/daily_transactions_summaries:latest .

# gcloud dataflow flex-template build \
# gs://dataflow-templates-financial-transactions/templates/daily_transactions_summaries.json \
# --image=gcr.io/cloud-sql-big-table-data-flow/daily_transactions_summaries:latest \
# --sdk-language=PYTHON \
# --metadata-file=metadata.json
