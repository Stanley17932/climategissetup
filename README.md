# Climate TRACE ingestion pipeline

Daily pull of Climate TRACE emissions data for four source categories, landed as
raw JSON in Azure Blob Storage for downstream (Databricks) processing.

## What it does

`.github/workflows/climatetrace-ingest.yml` runs every day at **06:00 UTC** (and
on manual dispatch). For each configured source it pulls the current year and the
previous year, validates the response, and uploads it.

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
be pruned by hand. The account currently has several long-lived IP rules from
earlier manual runs.

## Guards

Both guards fail the job (and skip the upload) rather than silently landing bad data:

- **Filter guard** — the response must contain only the requested
  sector(s)/subsector(s), and must be non-empty.
- **Volume guard** — a pull smaller than 50% of the previous run's record count is
  rejected. The previous count is read from blob metadata, so no download needed.
  Skipped automatically when no comparable earlier run exists.

A failing source does not stop the others; all pulls are attempted, then the job
exits non-zero with a summary.

## Running locally

```bash
pip install -r requirements.txt
az login                      # DefaultAzureCredential picks this up
export AZURE_STORAGE_ACCOUNT_URL=https://databrickssetup.blob.core.windows.net
python ingest.py
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
ingest.py                                  # the whole pipeline
requirements.txt
.github/workflows/climatetrace-ingest.yml  # schedule + OIDC + firewall handling
```
