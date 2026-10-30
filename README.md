# Climate TRACE ingestion pipeline

Daily pull of Climate TRACE emissions data for four source categories, landed as
raw JSON in Azure Blob Storage for downstream (Databricks) processing.

## What it does

`.github/workflows/climatetrace-ingest.yml` runs every day at **06:00 UTC** (and
on manual dispatch). For each configured source it pulls the current year and the
previous year, validates the response, and uploads it.

It then runs `warehouse.py`, which loads the landed JSON into the free Azure SQL
database and exports it to Parquet for Databricks — see
[Warehouse](#warehouse-azure-sql--parquet).

Data source: `https://api.climatetrace.org/v7/sources` (API v7), `gas=co2`.

| label | filter | why |
|---|---|---|
| `power` | `sectors=power` | top-level sector |
| `oil-and-gas-production-and-transport` | `subsectors=oil-and-gas-production,oil-and-gas-transport` | subsectors |
| `cement` | `subsectors=cement` | `manufacturing` subsector |
| `steel` | `subsectors=iron-and-steel` | `manufacturing` subsector |

> The API **silently ignores unknown filter values**. Passing `sectors=cement`
> returns *every* sector instead of an error. Only the ten real top-level sectors
> belong in `sectors`; everything else must go in `subsectors`. The ingest now
> fails loudly rather than landing unfiltered data if this regresses.

## Blob layout

```
landing/climatetrace/<label>/year=<year>/dt=<YYYY-MM-DD>/sources.json
```

- One blob per label/year/day, containing the raw API response as a JSON array.
- `dt=` is the run date (UTC), not the data's date.
- Re-running the same day replaces that day's blob.
- Each blob carries a `record_count` metadata field, used by the volume guard.

## Authentication

The job has **no stored storage credential**:

1. `azure/login` with GitHub OIDC → the `climatetrace-ingest-gh` service principal.
2. The principal holds two narrowly scoped roles on `databrickssetup`:

| role | scope | allows |
|---|---|---|
| `Climate TRACE storage firewall updater` (custom) | storage account | read/write account, `network-rule` changes. Explicitly **no** `listKeys`, `regenerateKey`, or delete. |
| `Climate TRACE landing writer` (custom) | `landing` container | blob read, write, add. **No** delete, no key access. |

`ingest.py` authenticates with `DefaultAzureCredential` against
`AZURE_STORAGE_ACCOUNT_URL`.

### The firewall

The storage account denies network access by default. GitHub-hosted runners have
random egress IPs, so each run:

1. finds its own egress IP,
2. adds a temporary `Allow` rule (retried, then verified to have landed),
3. waits until the storage API is actually reachable,
4. runs the ingest,
5. removes the rule — always, even if the job failed.

If the cleanup ever fails, the run emits a warning with the leftover IP so it can
be pruned by hand. In practice every run has cleaned up after itself, including
the one that failed mid-way. The account keeps `defaultAction: Deny`, one VNet
rule for the dev subnet, and a single allow rule for the developer's current IP —
old manual rules were pruned.

## Guards

Both guards fail the job (and skip the upload) rather than silently landing bad data:

- **Filter guard** — the response must contain only the requested
  sector(s)/subsector(s), and must be non-empty.
- **Volume guard** — a pull smaller than 50% of the previous run's record count is
  rejected. The previous count is read from blob metadata, so no download needed.
  Skipped automatically when no comparable earlier run exists.

A failing source does not stop the others; all pulls are attempted, then the job
exits non-zero with a summary.

## Warehouse (Azure SQL -> Parquet)

`warehouse.py` is the landing -> warehouse -> curated half of the pipeline:

```
python warehouse.py load     # newest blob per (source, year) -> dbo.climatetrace_sources
python warehouse.py export   # table -> parquet in the `curated` container
```

`load` is idempotent per `(year, gas)`: the year's rows are deleted and re-inserted
inside one transaction, so a re-run replaces rather than appends. The primary key is
`(id, year, gas)`; the API occasionally returns the same asset twice across a page
boundary, and those duplicates are collapsed (33,104 pulled -> 33,102 stored).

Two constraints shaped the write path, and both are worth knowing before "optimising" it:

- **`executemany` is not viable.** pyodbc sends a round trip per row; against this
  serverless database that measured ~108 ms/row, i.e. ~30 minutes per year. A
  parameterised multi-row `VALUES` list is no better - 21 columns means SQL Server's
  2,100-parameter ceiling caps it at 99 rows a statement, which is still 167 round
  trips per year.
- **One `OPENJSON` statement per chunk, 5,000 rows.** A single JSON parameter, shredded
  server-side. The cost is then just bytes on the wire, not round trips.

Measured on this database: 16,551 rows took 86 s, of which the clustered-index
build was 0.8 s. Nearly all of that was a slow client uplink (~75 KB/s); from a
GitHub runner the same load is seconds.

`export` writes one Parquet file per year to
`curated/climatetrace_sources/year=<year>/sources.parquet`, because Databricks Free
Edition compute **cannot** reach `*.database.windows.net` over the internet - its
serverless compute is restricted to a set of trusted domains, and Free Edition has no
egress settings to change. Parquet in blob storage is the bridge.

### SQL access

The same OIDC principal used for storage is a **contained database user** in
`free-sql-db-9295428`, added with no server-level login:

| grant | why |
|---|---|
| `db_datareader`, `db_datawriter` | `SELECT` for the export, `DELETE`/`INSERT` for the load |
| `CREATE TABLE` | `load` creates `dbo.climatetrace_sources` on first run |
| `ALTER ON SCHEMA::dbo` | lets `load` evolve that table |

There is no SQL password anywhere. `sql_connection()` takes an Entra token via
`DefaultAzureCredential` (the Azure CLI credential that `azure/login` seeds in CI),
and retries on the `40613`/`233` family because the free database is serverless and
**auto-pauses when idle** - the first connection of every daily run lands while it is
resuming.

The SQL server firewall denies by default too, so the workflow adds a
`github-actions-<ip>` rule alongside the storage rule and removes it afterwards.
Locally, your own IP needs a rule on both (and on the storage account).

## Running locally

```bash
pip install -r requirements.txt
az login                      # DefaultAzureCredential picks this up
export AZURE_STORAGE_ACCOUNT_URL=https://databrickssetup.blob.core.windows.net
python ingest.py

export AZURE_SQL_SERVER=stan-devsql.database.windows.net
export AZURE_SQL_DATABASE=free-sql-db-9295428
python warehouse.py load      # needs ODBC Driver 18 installed locally too
python warehouse.py export
```

Your IP must be allowed through the storage firewall, otherwise you get
`AuthorizationFailure`:

```bash
az storage account network-rule add -n databrickssetup -g DefaultResourceGroup-CCAN \
  --ip-address "$(curl -fsS https://api.ipify.org)"
# ... and remove it afterwards
```

`AZURE_STORAGE_CONNECTION_STRING` is still honoured as a fallback for accounts that
allow shared keys, but is not used in CI. To pull one source only, run locally:

```python
import ingest
data = ingest.fetch_climatetrace(ingest.SOURCES[0], 2025)
```

## Repository layout

```
ingest.py                                  # pull -> landing blobs
warehouse.py                               # landing -> Azure SQL -> curated Parquet
requirements.txt
.github/workflows/climatetrace-ingest.yml  # schedule + OIDC + firewall handling
```
