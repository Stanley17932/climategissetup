"""Push the exported Parquet snapshot into a Databricks Unity Catalog volume.

Databricks Free Edition is serverless-only with outbound internet restricted to
a set of trusted domains, and it has no private networking. The storage account
denies by default, so Free Edition cannot read `curated` (or anything else in
the account) over the internet. Pushing from CI inverts the direction and needs
no inbound access at all.

    python databricks_push.py --bootstrap        # create schema + volume, once
    python databricks_push.py --from=dist        # upload + refresh the table
    python databricks_push.py --from=dist --dry-run

Environment:
    DATABRICKS_HOST   https://dbc-xxxxxxxx-xxxx.cloud.databricks.com
    DATABRICKS_TOKEN  PAT scoped to files, unity-catalog and sql
    DATABRICKS_VOLUME catalog.schema.volume   (default main.climatetrace.raw)
    DATABRICKS_TABLE  catalog.schema.table    (default main.climatetrace.sources)
"""

import os
import sys
import time

import requests

DEFAULT_VOLUME = "main.climatetrace.raw"
DEFAULT_TABLE = "main.climatetrace.sources"
REMOTE_DIR = "climatetrace_sources"
TIMEOUT = 60


def env(name, default=None):
    value = os.environ.get(name)
    return value if value else default


def split_name(full_name, parts):
    bits = full_name.split(".")
    if len(bits) != parts:
        raise SystemExit(
            f"expected {parts} dotted parts in '{full_name}', got {len(bits)}"
        )
    return bits


class Databricks:
    def __init__(self, host, token, dry_run=False):
        host = host.rstrip("/")
        if not host.startswith("https://"):
            host = "https://" + host
        self.host = host
        self.token = token
        self.dry_run = dry_run

    def call(self, method, path, *, body=None, data=None, stream=False):
        url = f"{self.host}{path}"
        if self.dry_run and method in ("PUT", "POST", "PATCH", "DELETE"):
            print(f"  [dry-run] {method} {path} ({len(data) if data else 0} bytes)")
            return {"dry_run": True}

        headers = {"Authorization": f"Bearer {self.token}"}
        if data is not None:
            headers["Content-Type"] = "application/octet-stream"

        for attempt in range(1, 4):
            resp = requests.request(
                method,
                url,
                headers=headers,
                json=body,
                data=data,
                timeout=TIMEOUT,
                stream=stream,
            )
            # 429 is the per-workspace API rate limit; back off and retry.
            if resp.status_code == 429 and attempt < 3:
                time.sleep(5 * attempt)
                continue
            break

        if resp.status_code >= 400:
            raise SystemExit(f"{method} {path} -> {resp.status_code}: {resp.text[:400]}")
        if stream:
            return resp
        return resp.json() if resp.content else {}

    # --- Unity Catalog ----------------------------------------------------

    def ensure_schema_and_volume(self, catalog, schema, volume):
        try:
            self.call(
                "POST",
                "/api/2.1/unity-catalog/schemas",
                body={"catalog_name": catalog, "name": schema},
            )
            print(f"  created schema {catalog}.{schema}")
        except SystemExit as exc:
            if "already exists" not in str(exc).lower():
                raise
            print(f"  schema {catalog}.{schema} exists")

        try:
            self.call(
                "POST",
                "/api/2.1/unity-catalog/volumes",
                body={
                    "catalog_name": catalog,
                    "schema_name": schema,
                    "name": volume,
                    "volume_type": "MANAGED",
                },
            )
            print(f"  created volume {catalog}.{schema}.{volume}")
        except SystemExit as exc:
            if "already exists" not in str(exc).lower():
                raise
            print(f"  volume {catalog}.{schema}.{volume} exists")

    # --- files ------------------------------------------------------------

    def put_directory(self, volume_path):
        self.call("PUT", f"/api/2.0/fs/directories{volume_path}/")

    def put_file(self, volume_path, payload):
        self.call(
            "PUT",
            f"/api/2.0/fs/files{volume_path}?overwrite=true",
            data=payload,
        )

    def file_size(self, volume_path):
        resp = self.call("HEAD", f"/api/2.0/fs/files{volume_path}", stream=True)
        return int(resp.headers.get("Content-Length", -1))

    # --- SQL --------------------------------------------------------------

    def warehouse_id(self):
        warehouses = self.call("GET", "/api/2.0/sql/warehouses").get("warehouses", [])
        live = [w for w in warehouses if w.get("state") != "DELETED"]
        if not live:
            raise SystemExit("no SQL warehouse found in the workspace")
        # Free Edition has exactly one; prefer it if it is already running.
        live.sort(key=lambda w: w.get("state") != "RUNNING")
        return live[0]["id"]

    def run_sql(self, statement, wait_seconds=900):
        if self.dry_run:
            print(f"  [dry-run] SQL:\n{statement}")
            return

        warehouse = self.warehouse_id()

        result = self.call(
            "POST",
            "/api/2.0/sql/statements",
            body={
                "warehouse_id": warehouse,
                "statement": statement,
                "wait_timeout": "50s",
            },
        )
        deadline = time.time() + wait_seconds
        while result.get("status", {}).get("state") in ("PENDING", "RUNNING"):
            if time.time() > deadline:
                raise SystemExit("SQL statement did not finish within the deadline")
            time.sleep(5)
            result = self.call(
                "GET", f"/api/2.0/sql/statements/{result['statement_id']}"
            )

        state = result.get("status", {}).get("state")
        if state != "SUCCEEDED":
            message = result.get("status", {}).get("error", {}).get("message", state)
            raise SystemExit(f"SQL failed ({state}): {message}")
        print("  table refreshed")


def local_snapshots(directory):
    if not os.path.isdir(directory):
        raise SystemExit(f"{directory} does not exist - run `warehouse.py export --out=...`")
    found = []
    for name in sorted(os.listdir(directory)):
        # warehouse.py writes climatetrace_sources_year=YYYY.parquet
        if not name.endswith(".parquet") or "_year=" not in name:
            continue
        year = name.split("_year=", 1)[1].split(".", 1)[0]
        found.append((year, os.path.join(directory, name)))
    if not found:
        raise SystemExit(f"no climatetrace_sources_year=*.parquet files in {directory}")
    return found


def main(argv):
    args = argv[1:]
    source = next((a.split("=", 1)[1] for a in args if a.startswith("--from=")), None)
    dry_run = "--dry-run" in args
    bootstrap = "--bootstrap" in args
    refresh = "--no-refresh" not in args

    host = env("DATABRICKS_HOST")
    token = env("DATABRICKS_TOKEN")
    if not host or not token:
        raise SystemExit("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")

    volume_name = env("DATABRICKS_VOLUME", DEFAULT_VOLUME)
    table_name = env("DATABRICKS_TABLE", DEFAULT_TABLE)
    catalog, schema, volume = split_name(volume_name, 3)
    split_name(table_name, 3)  # fail early on a malformed table name

    api = Databricks(host, token, dry_run=dry_run)
    print(f"workspace {api.host} | volume {volume_name} | table {table_name}")

    if bootstrap:
        api.ensure_schema_and_volume(catalog, schema, volume)

    if source:
        snapshots = local_snapshots(source)
        root = f"/Volumes/{catalog}/{schema}/{volume}/{REMOTE_DIR}"
        for year, path in snapshots:
            remote = f"{root}/year={year}/sources.parquet"
            with open(path, "rb") as handle:
                payload = handle.read()
            api.put_directory(f"{root}/year={year}")
            api.put_file(remote, payload)
            size = len(payload) if dry_run else api.file_size(remote)
            print(f"  uploaded {remote} ({size / 1024:.0f} KB)")

        if refresh:
            pattern = f"/Volumes/{catalog}/{schema}/{volume}/{REMOTE_DIR}/*/*.parquet"
            api.run_sql(
                f"CREATE OR REPLACE TABLE {table_name} AS\n"
                f"SELECT * FROM parquet.`{pattern}`"
            )

    if not source and not bootstrap:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    main(sys.argv)
