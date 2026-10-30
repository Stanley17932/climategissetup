import os
import sys
import json
import time
import requests
from datetime import datetime, timezone
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError, HttpResponseError
from dotenv import load_dotenv

load_dotenv()

CONTAINER_NAME = "landing"
GAS = "co2"
API_URL = "https://api.climatetrace.org/v7/sources"
PAGE_SIZE = 1000
MAX_RETRIES = 5

# A pull that shrinks by more than this fraction versus the previous run is
# treated as a failure: a truncated page or an API change must not overwrite
# good data with a partial file.
MIN_VOLUME_RATIO = 0.5

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


class DataQualityError(RuntimeError):
    """The API returned something we are not willing to land in the blob store."""


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


def validate_pull(source, year, data):
    """Refuse to upload a pull that came back empty or unfiltered."""
    if not data:
        raise DataQualityError("API returned no rows")

    expected_sectors = set(source.get("sectors", []))
    expected_subsectors = set(source.get("subsectors", []))
    if not (expected_sectors or expected_subsectors):
        return

    if expected_sectors:
        seen = {r.get("sector") for r in data if r.get("sector")}
        bad = seen - expected_sectors
        if bad:
            raise DataQualityError(
                f"response contains unrequested sectors {sorted(bad)} "
                "- the API ignored the filter"
            )

    if expected_subsectors:
        seen = {r.get("subsector") for r in data if r.get("subsector")}
        bad = seen - expected_subsectors
        if bad:
            raise DataQualityError(
                f"response contains unrequested subsectors {sorted(bad)} "
                "- the API ignored the filter"
            )


def blob_path_for(label, year, today):
    return f"climatetrace/{label}/year={year}/dt={today}/sources.json"


def previous_record_count(container, label, year, exclude_path):
    """Record count of the most recent earlier run, read from blob metadata.

    Metadata avoids downloading the previous payload just to count it.
    Returns None when there is no comparable earlier run.
    """
    prefix = f"climatetrace/{label}/year={year}/"
    latest = None
    for blob in container.list_blobs(name_starts_with=prefix):
        if blob.name == exclude_path:
            continue
        if latest is None or blob.last_modified > latest.last_modified:
            latest = blob

    if latest is None:
        return None

    try:
        metadata = container.get_blob_client(latest.name).get_blob_properties().metadata
        return int(metadata["record_count"])
    except (KeyError, TypeError, ValueError):
        # Written by an older version of this job, before counts were stamped.
        return None


def check_volume(container, label, year, data, blob_path):
    previous = previous_record_count(container, label, year, blob_path)
    if previous is None:
        return
    if len(data) < previous * MIN_VOLUME_RATIO:
        raise DataQualityError(
            f"only {len(data)} rows, down from {previous} "
            f"({len(data) / previous:.0%} of the last run, floor is "
            f"{MIN_VOLUME_RATIO:.0%}) - refusing to overwrite"
        )
    print(f"  volume ok: {len(data)} rows vs {previous} last run")


def build_blob_service():
    """Authenticate to storage.

    In CI the job has no shared key: it signs in with OIDC and uses that
    identity. A connection string is still honoured for local development
    against an account that allows shared keys.
    """
    conn_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if conn_str:
        return BlobServiceClient.from_connection_string(conn_str)

    account_url = os.environ.get("AZURE_STORAGE_ACCOUNT_URL")
    if not account_url:
        raise SystemExit(
            "Set AZURE_STORAGE_ACCOUNT_URL (https://<account>.blob.core.windows.net) "
            "or AZURE_STORAGE_CONNECTION_STRING"
        )
    return BlobServiceClient(account_url=account_url,
                             credential=DefaultAzureCredential())


def ensure_container(blob_service):
    container = blob_service.get_container_client(CONTAINER_NAME)
    try:
        container.create_container()
        print(f"Created container '{CONTAINER_NAME}'")
    except ResourceExistsError:
        pass
    except HttpResponseError as exc:
        # Our role is scoped to the container itself, so it cannot create it.
        # That is expected: the container already exists.
        if exc.status_code != 403:
            raise
    return container


def upload_to_blob(container, data, label, year, blob_path):
    container.get_blob_client(blob_path).upload_blob(
        json.dumps(data),
        overwrite=True,
        metadata={"record_count": str(len(data))},
    )
    print(f"Uploaded {blob_path} ({len(data)} records)")


def main():
    # Keep stdout/stderr in order: failures are reported as they happen.
    sys.stdout.reconfigure(line_buffering=True)

    blob_service = build_blob_service()
    container = ensure_container(blob_service)

    current_year = datetime.now(timezone.utc).year
    years_to_pull = [current_year, current_year - 1]  # cover reporting lag
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    failures = []
    for source in SOURCES:
        for year in years_to_pull:
            label = source["label"]
            blob_path = blob_path_for(label, year, today)
            try:
                data = fetch_climatetrace(source, year)
                print(f"{label} / {year}: {len(data)} records")
                validate_pull(source, year, data)
                check_volume(container, label, year, data, blob_path)
                upload_to_blob(container, data, label, year, blob_path)
            except Exception as exc:
                # Keep going so one bad source does not hide the others,
                # then fail the job so the problem is visible.
                print(f"FAILED {label}/{year}: {type(exc).__name__}: {exc}", file=sys.stderr)
                failures.append(f"{label}/{year}: {exc}")

    if failures:
        print(f"\n{len(failures)} pull(s) failed:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
