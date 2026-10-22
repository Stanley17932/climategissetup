import os
import json
import time
import requests
from datetime import datetime, timezone
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError
from dotenv import load_dotenv

load_dotenv()

CONTAINER_NAME = "landing"
GAS = "co2"
API_URL = "https://api.climatetrace.org/v7/sources"
PAGE_SIZE = 1000
MAX_RETRIES = 5

# The Climate TRACE API silently IGNORES unknown filter values: if you pass
# sectors=cement it returns every sector instead of failing. Only these
# top-level sector ids are real:
#   fluorinated-gases, forestry-and-land-use, buildings, manufacturing,
#   mineral-extraction, waste, fossil-fuel-operations, agriculture,
#   transportation, power
# "cement", "steel" and "oil-and-gas-production-and-transport" are subsectors,
# so they must go in the `subsectors` parameter instead.
#
# `label` is what we use in the blob path, so keep it stable for downstream jobs.
SOURCES = [
    {"label": "power", "sectors": ["power"]},
    {
        "label": "oil-and-gas-production-and-transport",
        "subsectors": ["oil-and-gas-production", "oil-and-gas-transport"],
    },
    {"label": "cement", "subsectors": ["cement"]},
    {"label": "steel", "subsectors": ["iron-and-steel"]},
]


def _get(url, params):
    """GET with retry/backoff on rate limits and transient server errors."""
    delay = 2
    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.get(url, params=params, timeout=60)
        if resp.status_code in (429, 500, 502, 503, 504):
            if attempt == MAX_RETRIES:
                resp.raise_for_status()
            print(f"  HTTP {resp.status_code}, retrying in {delay}s "
                  f"(attempt {attempt}/{MAX_RETRIES})")
            time.sleep(delay)
            delay = min(delay * 2, 60)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError("unreachable")


def fetch_climatetrace(source, year, limit=PAGE_SIZE):
    all_results = []
    offset = 0
    while True:
        params = {"gas": GAS, "year": year, "limit": limit, "offset": offset}
        if "sectors" in source:
            params["sectors"] = ",".join(source["sectors"])
        if "subsectors" in source:
            params["subsectors"] = ",".join(source["subsectors"])

        batch = _get(API_URL, params)

        # The API returns `null` (not []) once you page past the end.
        if not batch or not isinstance(batch, list):
            break

        all_results.extend(batch)
        if len(batch) < limit:
            break
        offset += limit

    return all_results


def warn_on_unexpected(source, data):
    """Guard against the API silently returning unfiltered data."""
    expected_sectors = set(source.get("sectors", []))
    expected_subsectors = set(source.get("subsectors", []))
    if not (expected_sectors or expected_subsectors):
        return

    seen_sectors = {r.get("sector") for r in data}
    seen_subsectors = {r.get("subsector") for r in data}
    bad_sectors = seen_sectors - expected_sectors if expected_sectors else set()
    bad_subsectors = seen_subsectors - expected_subsectors if expected_subsectors else set()

    if bad_sectors or bad_subsectors:
        print(f"  WARNING: unexpected values in response "
              f"(sectors={sorted(bad_sectors)}, "
              f"subsectors={sorted(bad_subsectors)}) - filter may be ignored")


def ensure_container(blob_service):
    container = blob_service.get_container_client(CONTAINER_NAME)
    try:
        container.create_container()
        print(f"Created container '{CONTAINER_NAME}'")
    except ResourceExistsError:
        pass
    return container


def upload_to_blob(container, data, label, year):
    if not data:
        print(f"No data for {label} / {year}, skipping upload")
        return
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    blob_path = f"climatetrace/{label}/year={year}/dt={today}/sources.json"
    container.get_blob_client(blob_path).upload_blob(
        json.dumps(data), overwrite=True
    )
    print(f"Uploaded {blob_path} ({len(data)} records)")


def main():
    conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    blob_service = BlobServiceClient.from_connection_string(conn_str)
    container = ensure_container(blob_service)

    current_year = datetime.now(timezone.utc).year
    years_to_pull = [current_year, current_year - 1]  # cover reporting lag

    for source in SOURCES:
        for year in years_to_pull:
            data = fetch_climatetrace(source, year)
            print(f"{source['label']} / {year}: {len(data)} records")
            warn_on_unexpected(source, data)
            upload_to_blob(container, data, source["label"], year)


if __name__ == "__main__":
    main()
