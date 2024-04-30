# run every day to delete all data from s3
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
    filename=f"{os.environ['LOGS_FOLDER']}/01_flush_log_{dt.datetime.now().strftime('%Y%m%d')}.log",
    format=log_fmt,
    encoding="utf-8",
    level=lg.INFO
)

lg.info("Obtaining boto3 client.")
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
    aws_secret_access_key=os.environ["AWS_SECRET_KEY"]
)

BUCKET = os.environ["S3_BUCKET"]
PREFIX = os.environ['S3_REALTIME_FOLDER']

lg.info("Listing files to delete.")
response = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)

for object in response['Contents']:
    lg.info(f"Deleting, {object['Key']}")
    s3.delete_object(Bucket=BUCKET, Key=object["Key"])
