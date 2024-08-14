import json
from google.api_core import retry
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.bigquery.exceptions import BigQueryError

# Create clients
storage_client = storage.Client()
bq_client = bigquery.Client()

# Bigquery variables
dataset_name = "weather"
table_name = "tbl_condition"


def send_to_bq(data, ctx):
    """Transform and load data from cloud storage into BQ.
    This function is executed whenever a file is added to cloud storage

    :param data: dict File data
    :param ctx: google.cloud.functions.Context Metadata for the event
    :return:
    """

    # File metadata
    bucket_name = data["bucket"]
    file_name = data["name"]

    # Load file from bucket
    blob = storage_client.get_bucket(bucket_name).blob(file_name)

    # Transform and load
    raw_json = json.loads(blob.download_as_string())

    for j in raw_json:
        # Transform data by adding Celsius temp
        j["temperature_c"] = round((j["temperature"] - 32) * (5/9), 1)

        # Rename temperature column (create new key while deleting old key)
        j["temperature_f"] = j.pop("temperature")

        # Load to BQ data warehouse
        table = bq_client.dataset(dataset_name).table(table_name)
        errors = bq_client.insert_rows_json(table,
                                            json_rows=[j],
                                            row_ids=[file_name + j["zip_code"]],
                                            retry=retry.Retry(deadline=30))

        # Error handling
        if errors:
            raise BigQueryError(errors)

    # Delete the file to free up storage
    blob.delete()
