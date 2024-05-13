import requests
import json
import snowflake.connector
import os
from dotenv import load_dotenv
import datetime as dt
import logging as lg

load_dotenv()


def read_sql_file(path_to_sql):
    with open(path_to_sql, 'r') as file:
        file_content = file.read()
    return file_content


log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
lg.basicConfig(
    filename=f"{os.environ['LOGS_FOLDER']}/10_extract_log_{dt.datetime.now().strftime('%Y%m%d')}.log",
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


lg.info(f"Connecting to Snowflake")
with snowflake.connector.connect(
        user=os.environ["SFK_USER"],
        password=os.environ["SFK_PASSWORD"],
        account=os.environ["SFK_ACCOUNT"],
        database=os.environ["SFK_DATABASE"],
        schema=os.environ["SFK_SCHEMA"],
        role=os.environ["SFK_ROLE"],
        warehouse=os.environ["SFK_WAREHOUSE"]
    ) as cnx:
    cur = cnx.cursor()
    lg.info(f"Executing PUT command.")
    cur.execute(f"PUT file://{upload_file_path} @{os.environ['SFK_STAGE']};").fetchone()
    lg.info("File loaded to internal stage successfully.")

    copy_statement = read_sql_file("sql_scripts/01.1_aux_copy_statement.sql")
    lg.info("Executing copy statement.")
    cur.execute(copy_statement)

lg.info(f"Removing local file: {upload_file_path}")
os.remove(upload_file_path)
