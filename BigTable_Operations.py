# Regular imports 
import json
import random
import string
from collections import defaultdict
from datetime import datetime
from io import BytesIO

# Import BigTable Libraries

from google.cloud import bigtable
from google.cloud.bigtable import row_filters
from google.cloud.bigtable.row import DirectRow

# Import AVRO and PubSub Libraries
import avro.schema
from avro.io import DatumWriter, BinaryEncoder
from google.cloud import pubsub_v1
from google.pubsub_v1.types.schema import Schema, Encoding, GetSchemaRequest, SchemaView
from google.pubsub_v1.services.publisher import PublisherClient
from google.pubsub_v1.services.schema_service import SchemaServiceClient
from google.pubsub_v1.types.pubsub import PubsubMessage, PublishRequest

# BigTable credentials

PROJECT_ID  = "cloud-sql-big-table-data-flow"
BIGTABLE_INSTANCE_ID = "fin-bt"
BIGTABLE_TABLE_ID    = "txn_tbl"

bigtable_client   = bigtable.Client(project=PROJECT_ID, admin=True)
bigtable_instance = bigtable_client.instance(BIGTABLE_INSTANCE_ID)
bigtable_table    = bigtable_instance.table(BIGTABLE_TABLE_ID)

# Topic credentials
TOPIC_ID = "financial-transactions-topic"

def fetch_topic_schema_and_encoding(project_id: str, topic_id: str):
    publisher    = PublisherClient()
    topic_path   = publisher.topic_path(project_id, topic_id)
    topic        = publisher.get_topic(request={"topic": topic_path})
    schema_name  = topic.schema_settings.schema
    encoding     = topic.schema_settings.encoding
    schema_client = SchemaServiceClient()
    req = GetSchemaRequest(
        name=schema_name,
        view=SchemaView.FULL
    )
    schema_obj   = schema_client.get_schema(request=req)
    avro_schema  = avro.schema.parse(schema_obj.definition)
    return avro_schema, encoding, topic_path


def scan_all():
    print("\n--- Scan all rows ---")
    partial = bigtable_table.read_rows()
    partial.consume_all()
    for row_key, row in partial.rows.items():
        print(row_key.decode(), {
            f"{cf}:{col.decode()}": cell[0].value.decode()
            for cf, cols in row.cells.items()
            for col, cell in cols.items()
        })

def mark_as_published_to_pubsub(row_key):
    row = DirectRow(row_key=row_key.encode("utf-8"))
    row.set_cell("cf_latest", "passed_to_pubsub", b"True")
    bigtable_table.mutate_rows([row])

def send_message_to_pubsub_topic():
    # PubSub topic Metadata
    avro_schema, encoding, topic_path = fetch_topic_schema_and_encoding(PROJECT_ID, TOPIC_ID)     
    publisher = pubsub_v1.PublisherClient() 

    partial = bigtable_table.read_rows()
    partial.consume_all()
    for row_key, row in partial.rows.items():
        row_key = row_key.decode()
        raw_row = {
            f"{cf}:{col.decode()}": cell[0].value.decode()
            for cf, cols in row.cells.items()
            for col, cell in cols.items()
        }
        try:
            sent_to_pubsub = raw_row['cf_latest:passed_to_pubsub'].lower() == 'true'
        except:
            print('passed_to_pub_sub column is missing, skipping')
            continue
        if not sent_to_pubsub:
            try:
                avro_record = {
                    "transaction_id": int(raw_row.get("cf_latest:transaction_id")),
                    "account_id": int(raw_row.get("cf_latest:account_id")),
                    "amount": float(raw_row.get("cf_latest:amount")),
                    "currency": raw_row.get("cf_latest:currency"),
                    "transaction_type": raw_row.get("cf_latest:transaction_type"),
                    "transaction_date": raw_row.get("cf_history:transaction_date"),
                    "description": raw_row.get("cf_history:description"),
                    "status": raw_row.get("cf_history:status"),
                    "created_at": raw_row.get("cf_history:created_at"),
                    "updated_at": raw_row.get("cf_history:updated_at")
                }
            
                if encoding == Encoding.BINARY:
                    buffer = BytesIO()
                    writer = DatumWriter(avro_schema)
                    encoder = BinaryEncoder(buffer)
                    writer.write(avro_record, encoder)
                    message_bytes = buffer.getvalue()
                elif encoding == Encoding.JSON:
                    message_bytes = json.dumps(avro_record).encode("utf-8")
                else:
                    raise RuntimeError('Unsupported input data formats')
                
                future = publisher.publish(topic_path, message_bytes)
                # Update record pubsub status
                mark_as_published_to_pubsub(row_key)
                print(f"✅ Published message ID: {future.result()} and marketd as sent in BigTable -> Wont be sent again unless updated")

            except ValueError as e:
                raise('Code has errors please review')
        else:
            pass
        
if __name__ == '__main__':
    send_message_to_pubsub_topic()
    



# def populate_message_to_pubsub():
#     mutations = []
#     for row in bigtable_table.read_rows():
#         direct_row = DirectRow(row.row_key)
#         direct_row.set_cell("cf_latest", "passed_to_pubsub", b"False")
#         mutations.append(direct_row)
#     bigtable_table.mutate_rows(mutations)