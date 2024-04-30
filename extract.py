import requests
import json
import boto3
import os
from dotenv import load_dotenv
import datetime as dt
import logging as lg

load_dotenv()

log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
lg.basicConfig(
    filename=f"{os.environ['LOGS_FOLDER']}/00_extract_log_{dt.datetime.now().strftime('%Y%m%d')}.log",
    format=log_fmt,
    encoding="utf-8",
    level=lg.INFO
)

upload_file_path = f"stations_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
bcn_od_url = "https://opendata-ajuntament.barcelona.cat/data/es/dataset/estat-estacions-bicing/resource/1b215493-9e63-4a12-8980-2d7e0fa19f85/download/recurs.json"
headers = {
  'Authorization': os.environ["API_KEY"]
}

lg.info("Querying Barcelona Open Data API")
response = requests.get(bcn_od_url, headers=headers)

lg.info(f"Saving json file to {upload_file_path}")
with open(upload_file_path, 'w') as f:
    json.dump(response.json()["data"]["stations"], f, indent=2)


target_file_name = f"{os.environ['S3_REALTIME_FOLDER']}/{upload_file_path}"
s3_bucket_name = os.environ["S3_BUCKET"]

lg.info("Obtaining boto3 client.")
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
    aws_secret_access_key=os.environ["AWS_SECRET_KEY"]
)

lg.info(f"Uploading file to S3, bucket: {s3_bucket_name}, filename: {target_file_name}")
s3.upload_file(
    Filename=upload_file_path,
    Bucket=s3_bucket_name,
    Key=target_file_name,
)

lg.info(f"Removing local file: {upload_file_path}")
os.remove(upload_file_path)
