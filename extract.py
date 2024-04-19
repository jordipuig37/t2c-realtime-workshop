import requests
import json
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient
import os
from dotenv import load_dotenv
import datetime as dt

load_dotenv()

upload_file_path = f"stations_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
bcn_od_url = "https://opendata-ajuntament.barcelona.cat/data/es/dataset/estat-estacions-bicing/resource/1b215493-9e63-4a12-8980-2d7e0fa19f85/download/recurs.json"
headers = {
  'Authorization': os.environ["API_KEY"]
}

response = requests.get(bcn_od_url, headers=headers)

with open(upload_file_path, 'w') as f:
    json.dump(response.json()["data"]["stations"], f, indent=2)


container_name = "bicing"
target_file_name = f"daily_data/{upload_file_path}"
adls_account_url = "https://t2crealtimeworkshop.blob.core.windows.net"
sas_token = os.environ["SAS_TOKEN"]

# Create the BlobServiceClient object
blob_service_client = BlobServiceClient(account_url=adls_account_url, credential=sas_token)

# Create a blob client using the local file name as the name for the blob
blob_client = blob_service_client.get_blob_client(container=container_name, blob=target_file_name)

# Upload the created file
with open(file=upload_file_path, mode="rb") as data:
    blob_client.upload_blob(data)
