"""Model the landing payloads into Azure SQL, then export them for Databricks.

Two steps, both idempotent:

    python warehouse.py load     # newest blob per (source, year) -> dbo.climatetrace_sources
    python warehouse.py export   # table -> Parquet in the `curated` container

Databricks Free Edition cannot reach Azure SQL (its serverless compute only
gets outbound access to trusted domains), so the export step is how the data
crosses over: Parquet is pushed to storage and dragged into a Unity Catalog
volume.
"""

import io
import json
import math
import os
import struct
import sys
import time

import pyodbc
from azure.core.exceptions import HttpResponseError, ResourceExistsError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

from ingest import SOURCES

load_dotenv()

LANDING_CONTAINER = "landing"
CURATED_CONTAINER = "curated"
TABLE = "dbo.climatetrace_sources"
SQL_SCOPE = "https://database.windows.net/.default"
SQL_COPT_SS_ACCESS_TOKEN = 1256

# Column widths are generous against the observed maximums (name=109 chars,
# units=24) so a slightly longer value never fails the load.
DDL = f"""
IF OBJECT_ID('{TABLE}', 'U') IS NULL
BEGIN
    CREATE TABLE {TABLE} (
        id                      BIGINT        NOT NULL,
        [year]                  SMALLINT      NOT NULL,
        gas                     VARCHAR(16)   NOT NULL,
        [name]                  NVARCHAR(1000) NULL,
        sector                  VARCHAR(64)   NOT NULL,
        subsector               VARCHAR(64)   NULL,
        country                 VARCHAR(10)   NULL,
        asset_type              VARCHAR(100)  NULL,
        source_type             VARCHAR(50)   NULL,
        latitude                FLOAT         NULL,
        longitude               FLOAT         NULL,
        emissions_quantity      FLOAT         NULL,
        emissions_factor        FLOAT         NULL,
        emissions_factor_units  NVARCHAR(200) NULL,
        activity                FLOAT         NULL,
        activity_units          NVARCHAR(200) NULL,
        capacity                FLOAT         NULL,
        capacity_units          NVARCHAR(200) NULL,
        capacity_factor         FLOAT         NULL,
        source_label            VARCHAR(64)   NOT NULL,
        partition_dt            DATE          NOT NULL,
        loaded_at               DATETIME2(0)  NOT NULL
                                CONSTRAINT df_climatetrace_loaded
                                DEFAULT SYSUTCDATETIME(),
        CONSTRAINT pk_climatetrace_sources PRIMARY KEY CLUSTERED (id, [year], gas)
    );
END
"""

# Rows are sent as one JSON document per chunk and shredded server-side by
# OPENJSON. The obvious alternative - executemany - is not viable here: pyodbc
# sends a round trip per row, which cost ~108 ms/row against this serverless
# database (~30 min per year). A parameterised multi-row VALUES list is also out
# because it hits SQL Server's 2,100-parameter ceiling (21 columns => 99 rows).
# OPENJSON passes a single parameter and lets the server do the set-based insert.
CHUNK_ROWS = 5000

INSERT_SQL = f"""
INSERT INTO {TABLE} (
    id, [year], gas, [name], sector, subsector, country, asset_type, source_type,
    latitude, longitude, emissions_quantity, emissions_factor,
    emissions_factor_units, activity, activity_units, capacity, capacity_units,
    capacity_factor, source_label, partition_dt
)
SELECT
    id, [year], gas, [name], sector, subsector, country, asset_type, source_type,
    latitude, longitude, emissions_quantity, emissions_factor,
    emissions_factor_units, activity, activity_units, capacity, capacity_units,
    capacity_factor, source_label, CONVERT(date, partition_dt)
FROM OPENJSON(?) WITH (
    id                     BIGINT        '$.id',
    [year]                 SMALLINT      '$.year',
    gas                    VARCHAR(16)   '$.gas',
    [name]                 NVARCHAR(1000) '$.name',
    sector                 VARCHAR(64)   '$.sector',
    subsector              VARCHAR(64)   '$.subsector',
    country                VARCHAR(10)   '$.country',
    asset_type             VARCHAR(100)  '$.asset_type',
    source_type            VARCHAR(50)   '$.source_type',
    latitude               FLOAT         '$.latitude',
    longitude              FLOAT         '$.longitude',
    emissions_quantity     FLOAT         '$.emissions_quantity',
    emissions_factor       FLOAT         '$.emissions_factor',
    emissions_factor_units NVARCHAR(200) '$.emissions_factor_units',
    activity               FLOAT         '$.activity',
    activity_units         NVARCHAR(200) '$.activity_units',
    capacity               FLOAT         '$.capacity',
    capacity_units         NVARCHAR(200) '$.capacity_units',
    capacity_factor        FLOAT         '$.capacity_factor',
    source_label           VARCHAR(64)   '$.source_label',
    partition_dt           VARCHAR(30)   '$.partition_dt'
)
"""

COLUMNS = [
    "id", "year", "gas", "name", "sector", "subsector", "country", "asset_type",
    "source_type", "latitude", "longitude", "emissions_quantity",
    "emissions_factor", "emissions_factor_units", "activity", "activity_units",
    "capacity", "capacity_units", "capacity_factor", "source_label",
    "partition_dt",
]

MAX_LEN = {
    "gas": 16, "name": 1000, "sector": 64, "subsector": 64, "country": 10,
    "asset_type": 100, "source_type": 50, "emissions_factor_units": 200,
    "activity_units": 200, "capacity_units": 200, "source_label": 64,
}


def _text(value, field):
    if value is None:
        return None
    text = str(value)
    return text[: MAX_LEN[field]]


def _num(value):
    """Coerce to a SQL-safe float/int; NaN and infinity are not storable."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            return None
        return value
    return None


def to_row(record, source_label, partition_dt):
    centroid = record.get("centroid") or {}
    return (
        record["id"],
        record["year"],
        _text(record.get("gas"), "gas"),
        _text(record.get("name"), "name"),
        _text(record.get("sector"), "sector"),
        _text(record.get("subsector"), "subsector"),
        _text(record.get("country"), "country"),
        _text(record.get("assetType"), "asset_type"),
        _text(record.get("sourceType"), "source_type"),
        _num(centroid.get("latitude")),
        _num(centroid.get("longitude")),
        _num(record.get("emissionsQuantity")),
        _num(record.get("emissionsFactor")),
        _text(record.get("emissionsFactorUnits"), "emissions_factor_units"),
        _num(record.get("activity")),
        _text(record.get("activityUnits"), "activity_units"),
        _num(record.get("capacity")),
        _text(record.get("capacityUnits"), "capacity_units"),
        _num(record.get("capacityFactor")),
        _text(source_label, "source_label"),
        partition_dt,
    )


def _odbc_driver():
    available = set(pyodbc.drivers())
    for name in ("ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"):
        if name in available:
            return name
    raise SystemExit(
        "No SQL Server ODBC driver found. On Debian/Ubuntu install msodbcsql18."
    )


# The free database is serverless and auto-pauses when idle, so the first
# connection of a run usually lands while it is still resuming and fails with
# 40613 "database not currently available". Retry until it wakes up.
TRANSIENT_SQL_ERRORS = ("40613", "4060", "40197", "933", "64", "233")


def sql_connection(retries=24, delay=10):
    """Connect with an Entra ID token - no SQL password anywhere.

    Works in CI because azure/login seeds the Azure CLI credential that
    DefaultAzureCredential picks up, and locally after `az login`.
    """
    server = os.environ.get("AZURE_SQL_SERVER")
    database = os.environ.get("AZURE_SQL_DATABASE")
    if not (server and database):
        raise SystemExit("Set AZURE_SQL_SERVER and AZURE_SQL_DATABASE")

    token = DefaultAzureCredential().get_token(SQL_SCOPE).token
    encoded = token.encode("utf-16-le")
    attrs = {SQL_COPT_SS_ACCESS_TOKEN: struct.pack("=i", len(encoded)) + encoded}
    conn_str = (
        f"Driver={{{_odbc_driver()}}};Server={server};Database={database};"
        "Encrypt=yes;TrustServerCertificate=no;"
    )

    for attempt in range(1, retries + 1):
        try:
            return pyodbc.connect(
                conn_str, attrs_before=attrs, timeout=120, autocommit=False
            )
        except pyodbc.Error as exc:
            message = str(exc)
            transient = any(code in message for code in TRANSIENT_SQL_ERRORS)
            if not transient or attempt == retries:
                raise
            print(
                f"  database is resuming, retrying in {delay}s "
                f"(attempt {attempt}/{retries})"
            )
            time.sleep(delay)
    raise RuntimeError("unreachable")


def newest_blob(container, label, year):
    prefix = f"climatetrace/{label}/year={year}/"
    latest = None
    for blob in container.list_blobs(name_starts_with=prefix):
        if latest is None or blob.last_modified > latest.last_modified:
            latest = blob
    return latest


def _partition_date(blob_name):
    # .../dt=YYYY-MM-DD/sources.json
    return blob_name.rsplit("dt=", 1)[1][:10]


def _chunks(items, size):
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _json_payload(row_chunk):
    """Serialise a chunk of positional rows the way INSERT_SQL's OPENJSON expects."""
    return json.dumps(
        [dict(zip(COLUMNS, row)) for row in row_chunk], default=str
    )


def load(conn, blob_service, years):
    container = blob_service.get_container_client(LANDING_CONTAINER)
    cur = conn.cursor()
    cur.execute(DDL)
    conn.commit()

    total = 0
    for year in years:
        rows = {}
        for source in SOURCES:
            label = source["label"]
            blob = newest_blob(container, label, year)
            if blob is None:
                print(f"  {label}/{year}: no blob, skipped")
                continue
            payload = json.loads(
                container.get_blob_client(blob.name).download_blob().readall()
            )
            partition_dt = _partition_date(blob.name)
            for record in payload:
                # The API occasionally returns the same asset twice across a
                # page boundary; the primary key is (id, year, gas), so the
                # later one simply replaces the earlier identical copy.
                rows[(record["id"], year, record.get("gas"))] = to_row(
                    record, label, partition_dt
                )
            print(f"  {label}/{year}: {len(payload)} records from {blob.name.split('/')[-2]}")

        if not rows:
            continue

        try:
            cur.execute("SET XACT_ABORT ON")
            cur.execute(
                f"DELETE FROM {TABLE} WHERE [year] = ? AND gas = ?", year, "co2"
            )
            rows_list = list(rows.values())
            for chunk in _chunks(rows_list, CHUNK_ROWS):
                cur.execute(INSERT_SQL, _json_payload(chunk))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        print(f"  {year}: loaded {len(rows)} rows")
        total += len(rows)

    cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
    print(f"table now holds {cur.fetchone()[0]} rows (loaded {total} this run)")


def export(conn, blob_service, out_dir=None):
    import pyarrow as pa
    import pyarrow.parquet as pq

    cur = conn.cursor()
    cur.execute(f"SELECT {', '.join(COLUMNS)} FROM {TABLE} ORDER BY [year], id")
    columns = [d[0] for d in cur.description]
    rows = [dict(zip(columns, r)) for r in cur.fetchall()]
    if not rows:
        raise SystemExit("table is empty - run `load` first")

    container = blob_service.get_container_client(CURATED_CONTAINER)
    try:
        container.create_container()
    except ResourceExistsError:
        pass
    except HttpResponseError as exc:
        # Creating a container is an account-level operation, so the CI identity -
        # which is scoped to this one container - is refused. That is fine as long
        # as the container exists; the upload below is the real permission test.
        if getattr(exc, "status_code", None) != 403:
            raise

    written = []
    for year in sorted({r["year"] for r in rows}):
        part = [r for r in rows if r["year"] == year]
        table = pa.Table.from_pylist(part)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        payload = buf.getvalue()

        name = f"climatetrace_sources/year={year}/sources.parquet"
        container.get_blob_client(name).upload_blob(payload, overwrite=True)
        written.append((name, len(part), len(payload)))

        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
            local = os.path.join(out_dir, f"climatetrace_sources_year={year}.parquet")
            with open(local, "wb") as handle:
                handle.write(payload)
            written[-1] = written[-1] + (local,)

    for entry in written:
        name, count, size = entry[:3]
        where = f" + {entry[3]}" if len(entry) > 3 else ""
        print(f"  {name}: {count} rows, {size / 1024:.0f} KB{where}")


def main(argv):
    if len(argv) < 2 or argv[1] not in ("load", "export"):
        raise SystemExit(__doc__)

    blob_service = BlobServiceClient(
        os.environ["AZURE_STORAGE_ACCOUNT_URL"],
        credential=DefaultAzureCredential(),
    )
    conn = sql_connection()
    try:
        if argv[1] == "load":
            from datetime import datetime, timezone

            now = datetime.now(timezone.utc).year
            years = [int(a.split("=")[1]) for a in argv[2:] if a.startswith("--year=")] or [
                now, now - 1
            ]
            load(conn, blob_service, years)
        else:
            out_dir = next(
                (a.split("=", 1)[1] for a in argv[2:] if a.startswith("--out=")), None
            )
            export(conn, blob_service, out_dir)
    finally:
        conn.close()


if __name__ == "__main__":
    main(sys.argv)
