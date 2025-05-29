import os
import json
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

# Constants
BLS_BASE_URL = "https://download.bls.gov/pub/time.series/pr/"
POP_API_URL = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
BUCKET_NAME = "rearcquestv2"
BLS_S3_PREFIX = "bls-data/"
POP_S3_KEY = "population-data/population.json"

s3 = boto3.client("s3")
headers = {"User-Agent": "Mozilla/5.0 (compatible; LambdaScript/1.0; +http://example.com)"}

def sync_bls_files():
    try:
        resp = requests.get(BLS_BASE_URL, headers=headers)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        files_to_sync = [
            link.get("href") for link in soup.find_all("a")
            if link.get("href") and not link.get("href").endswith("/")
        ]

        # Fetch existing files from S3
        paginator = s3.get_paginator("list_objects_v2")
        existing_files = set()
        for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=BLS_S3_PREFIX):
            for obj in page.get("Contents", []):
                existing_files.add(obj["Key"].replace(BLS_S3_PREFIX, ""))

        # Upload new/changed files
        files_synced = set()
        for fname in files_to_sync:
            filename = os.path.basename(fname)
            file_url = urljoin(BLS_BASE_URL, fname)
            file_resp = requests.get(file_url, headers=headers, stream=True)
            file_resp.raise_for_status()
            s3.upload_fileobj(file_resp.raw, BUCKET_NAME, BLS_S3_PREFIX + filename)
            files_synced.add(filename)

        # Delete removed files
        to_delete = existing_files - files_synced
        for fname in to_delete:
            s3.delete_object(Bucket=BUCKET_NAME, Key=BLS_S3_PREFIX + fname)

        return {
            "synced_files_count": len(files_synced),
            "deleted_files_count": len(to_delete)
        }
    except Exception as e:
        print(f"[ERROR] BLS sync failed: {e}")
        return {
            "synced_files_count": 0,
            "deleted_files_count": 0,
            "error": str(e)
        }

def fetch_and_store_population_data():
    try:
        response = requests.get(POP_API_URL)
        response.raise_for_status()
        data = response.json()
        json_bytes = json.dumps(data).encode("utf-8")
        s3.put_object(Bucket=BUCKET_NAME, Key=POP_S3_KEY, Body=json_bytes)
        print(f"[INFO] Uploaded population data to s3://{BUCKET_NAME}/{POP_S3_KEY}")
        return True
    except (requests.RequestException, ClientError, NoCredentialsError) as e:
        print(f"[ERROR] Population data fetch/upload failed: {e}")
        return False

def lambda_handler(event, context):
    bls_result = sync_bls_files()
    pop_success = fetch_and_store_population_data()

    return {
        "statusCode": 200 if pop_success else 500,
        "body": json.dumps({
            "bls_sync": bls_result,
            "population_upload_success": pop_success
        })
    }
