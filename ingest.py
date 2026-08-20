import os
import json
import requests
from datetime import datetime, timezone
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

CONTAINER_NAME = "landing"
SECTORS = ["power", "oil-and-gas-production-and-transport", "cement", "steel"]
GAS = "co2"

def fetch_climatetrace(sector, year, limit=1000):
    all_results = []
    offset = 0
    while True:
        resp = requests.get(
            "https://api.climatetrace.org/v7/sources",
            params={
                "sectors": sector,
                "gas": GAS,
                "year": year,
                "limit": limit,
                "offset": offset,
            },
            timeout=30
        )
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        all_results.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
    return all_results

def upload_to_blob(blob_service, data, sector, year):
    if not data:
        print(f"No data for {sector} / {year}, skipping upload")
        return
    container = blob_service.get_container_client(CONTAINER_NAME)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    blob_path = f"climatetrace/{sector}/year={year}/dt={today}/sources.json"
    container.get_blob_client(blob_path).upload_blob(
        json.dumps(data), overwrite=True
    )
    print(f"Uploaded {blob_path} ({len(data)} records)")

def main():
    conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    blob_service = BlobServiceClient.from_connection_string(conn_str)

    current_year = datetime.now(timezone.utc).year
    years_to_pull = [current_year, current_year - 1]  # cover reporting lag

    for sector in SECTORS:
        for year in years_to_pull:
            data = fetch_climatetrace(sector, year)
            upload_to_blob(blob_service, data, sector, year)

if __name__ == "__main__":
    main()