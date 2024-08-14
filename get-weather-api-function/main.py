import requests
import json
import time
import logging

from google.cloud import storage
from google.cloud import secretmanager
from typing import Dict


def get_weather(event: Dict, ctx):
    """Retrieve weather data from the VisualCrossing api and store in cloud storage
    bucket.

    :param event: Event payload
    :param ctx: google.cloud.functions.Context Metadata for the event
    """
    try:
        # Client creation
        storage_client = storage.Client()
        secrets_client = secretmanager.SecretManagerServiceClient()

        # Variable set up
        condition_list = []
        zip_code_list = ["92101", "92071", "98101"]
        project_id = "emiya-python-api-etl-warehouse"
        secret_id = "VisualCrossing_API_Key"
        bucket_id = "emiya_weather_visualcrossing"

        # API key retrieval from secrets manager
        name = (f"projects/{project_id}/secrets/{secret_id}/versions/1")
        response = secrets_client.access_secret_version(request={"name": name})
        api_key = response.payload.data.decode("UTF-8")

        # Retrieve and model the data
        for zip_code in zip_code_list:
            # API call
            r = requests.get(
                f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest"
                f"/services/timeline/{zip_code}?key={api_key}")
            r_json = json.loads(r.text)

            # Model data
            city_dict = {
                "condition_timestamp": r_json["days"]["datetime"],
                "zipcode": zip_code,
                "city": r_json["resolvedAddress"],
                "temperature": r_json["days"]["temp"],
                "condition": r_json["days"]["conditions"]
            }

            # Stage data
            condition_list.append(city_dict)

        # Store data in bucket
        bucket = storage_client.get_bucket(bucket_id)

        # Create a unique name for the file using unix timestamp
        blob = bucket.blob(f"{int(time.time())}.json")

        # Add data to storage as a json file
        blob.upload_from_string(
            data=json.dumps(condition_list),
            content_type="application/json"
        )

    except Exception as e:
        logging.error(e)
