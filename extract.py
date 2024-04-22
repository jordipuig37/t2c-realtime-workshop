import requests
import json
import boto3
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

target_file_name = f"daily_data/{upload_file_path}"
s3_bucket_name = os.environ["S3_BUCKET"]

s3 = boto3.client("s3")

s3.upload_file(
    Filename=upload_file_path,
    Bucket=s3_bucket_name,
    Key=f"real_time_data/{upload_file_path}",
)
