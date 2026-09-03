# Databricks Batch Mini Project — Investment & Trade Data

A batch lakehouse pipeline built on **Azure Databricks + Unity Catalog** that ingests daily trade files and produces governed, reconciled business metrics.

```
CSV files (customers, accounts, transactions)
        │  land in a Unity Catalog Volume (ADLS Gen2)
        ▼
   BRONZE  (raw + ingestion metadata: _ingested_at, _source_file, _batch_id)
        │  cast types, trim/upper-case, compute trade_value
        ▼
   Data Quality Gate ──► QUARANTINE (bad rows + _dq_reason)
        │  valid rows only
        ▼
   SILVER  (conformed: joined to accounts + customers)
        │  aggregate by trade_date, symbol, currency
        ▼
   GOLD    (gold_daily_position: buy/sell/gross value, net flow, trade count)
```

Every table is Delta Lake, versioned, and reconciled: row counts (bronze = silver + quarantine) and financial totals (silver gross value = gold gross value) are checked on every run.

## Repository structure
| Path | Purpose |
|---|---|
| `data/` | Sample source CSVs — `customers.csv`, `accounts.csv`, `transactions.csv` |
| `src/01_batch_pipeline.py` | Main pipeline. Written as a Databricks notebook (`# Databricks notebook source`) — import it directly |
| `sql/validation.sql` | Reconciliation and data-quality validation queries, run in a SQL warehouse |
| `tests/test_batch.py` | Local `pytest` unit tests for the transformation logic (no Databricks needed) |
| `resources/job.yml` | Databricks Asset Bundle resource definition — deploys the pipeline as a scheduled Lakeflow Job |

## Data quality rules
Rows failing any of these are written to `quarantine_trades` with a `_dq_reason`, not silently dropped:

| Rule | Reason code |
|---|---|
| `trade_id` not null | `NULL_TRADE_ID` |
| `account_id` not null | `NULL_ACCOUNT_ID` |
| `quantity` > 0 | `NON_POSITIVE_QUANTITY` |
| `price` > 0 | `NON_POSITIVE_PRICE` |
| `side` in (BUY, SELL) | `INVALID_SIDE` |
| no duplicate `trade_id` | `DUPLICATE_TRADE_ID` |

---

## 1. Azure resources you need to create

You need an Azure subscription with permission to create resource groups and register resource providers. Create these resources, in this order:

| # | Resource | Why | Notes |
|---|---|---|---|
| 1 | **Resource Group** | Container for everything below | e.g. `rg-databricks-batch-mini` |
| 2 | **Storage account (ADLS Gen2)** | Backs the Unity Catalog metastore and holds managed table data | Enable **hierarchical namespace** (this is what makes it ADLS Gen2, not blob storage). Create a container, e.g. `unity-catalog` |
| 3 | **Access Connector for Azure Databricks** | A managed identity Unity Catalog uses to read/write the storage account — no keys/secrets needed | Free resource; grant it **Storage Blob Data Contributor** on the storage account (or the container) in step 4 |
| 4 | **Azure Databricks workspace** | The workspace itself | Pricing tier must be **Premium** (Unity Catalog and job scheduling both require it) |
| 5 | *(Optional)* **Azure Key Vault** | Store secrets if you later connect external systems (e.g. notification webhooks) | Not required for the CSV-based flow in this project |

### Step-by-step (Azure Portal or CLI)

**Using the Azure Portal:**
1. Create the **Resource Group**.
2. Create the **Storage Account** in that group → Advanced tab → enable **"Enable hierarchical namespace"** → create a container named `unity-catalog`.
3. Create **Access Connector for Azure Databricks** in the same resource group/region.
4. On the storage account → **Access Control (IAM)** → **Add role assignment** → role **Storage Blob Data Contributor** → assign to the Access Connector's **managed identity** (search by the connector's name).
5. Create the **Azure Databricks workspace**, tier = **Premium**, same region as the storage account.

**Using Azure CLI (equivalent, faster):**
```bash
az group create -n rg-databricks-batch-mini -l eastus

az storage account create \
  -n stdbxbatchmini -g rg-databricks-batch-mini -l eastus \
  --sku Standard_LRS --kind StorageV2 --hierarchical-namespace true

az storage container create \
  --account-name stdbxbatchmini -n unity-catalog --auth-mode login

az databricks access-connector create \
  -g rg-databricks-batch-mini -n dbx-access-connector -l eastus \
  --identity-type SystemAssigned

# Grab the connector's principal id, then grant it storage access:
CONNECTOR_ID=$(az databricks access-connector show -g rg-databricks-batch-mini -n dbx-access-connector --query identity.principalId -o tsv)
STORAGE_ID=$(az storage account show -g rg-databricks-batch-mini -n stdbxbatchmini --query id -o tsv)

az role assignment create \
  --assignee "$CONNECTOR_ID" \
  --role "Storage Blob Data Contributor" \
  --scope "$STORAGE_ID"

az databricks workspace create \
  -g rg-databricks-batch-mini -n dbx-batch-mini-ws -l eastus \
  --sku premium
```

---

## 2. One-time Databricks account setup (Unity Catalog)

Unity Catalog is set up once per Databricks **account**, then attached to workspaces.

1. Go to the **Account Console** (`accounts.azuredatabricks.net`) — sign in with the same Azure AD/Entra ID identity that has owner access to the workspace.
2. **Catalog** → **Create Metastore**:
   - Region: same as your storage account.
   - ADLS Gen2 path: `abfss://unity-catalog@stdbxbatchmini.dfs.core.windows.net/`
   - Access Connector ID: the one created above.
3. **Assign** the metastore to your Databricks workspace.
4. In the workspace, grant yourself (or your group) `CREATE CATALOG` on the metastore, or have an admin run the catalog/schema/volume creation cell in the notebook for you (it's already handled by the pipeline — see below).

If you're using a **free trial / Databricks Community/Trial workspace with a pre-provisioned metastore**, you can skip straight to step 3 — a default metastore may already be attached.

---

## 3. Run the pipeline in Databricks

### Step 1 — Import the notebook
1. In the Databricks workspace, go to **Workspace** → your user folder → **Import**.
2. Upload `src/01_batch_pipeline.py` (Databricks recognizes the `# Databricks notebook source` header and imports it as a notebook with cells already split out).

### Step 2 — Get compute
1. Create a small **all-purpose cluster** (or use **Serverless compute** if available in your workspace) — no need for a big cluster, this is a mini dataset. Databricks Runtime 15.4 LTS or later.
2. Attach the notebook to the cluster.

### Step 3 — Set the widgets
At the top of the notebook, three widgets control where data lands:
- `catalog` (default `main`)
- `schema` (default `batch_mini_project`)
- `volume` (default `landing`)

Set them to whatever you want, or leave the defaults.

### Step 4 — Create the catalog/schema/volume
Run the first few cells — they execute:
```sql
CREATE CATALOG IF NOT EXISTS <catalog>
CREATE SCHEMA IF NOT EXISTS <catalog>.<schema>
CREATE VOLUME IF NOT EXISTS <catalog>.<schema>.<volume>
```
This needs `CREATE CATALOG` privilege on the metastore — if it fails, ask your workspace/account admin to grant it or to run this cell once.

### Step 5 — Upload the sample data
Upload the three files from `data/` into the volume path printed by the notebook (`/Volumes/<catalog>/<schema>/<volume>`):
- Easiest: **Catalog Explorer** → navigate to the volume → **Upload to this volume** → drag in `customers.csv`, `accounts.csv`, `transactions.csv`.
- Or via Databricks CLI:
  ```bash
  databricks fs cp data/customers.csv dbfs:/Volumes/<catalog>/<schema>/<volume>/customers.csv
  databricks fs cp data/accounts.csv dbfs:/Volumes/<catalog>/<schema>/<volume>/accounts.csv
  databricks fs cp data/transactions.csv dbfs:/Volumes/<catalog>/<schema>/<volume>/transactions.csv
  ```

### Step 6 — Run all cells
**Run All** (top to bottom). The notebook will:
1. Read the three CSVs with explicit schemas.
2. Write `customers` and `accounts` as Delta tables.
3. Build **Bronze** (`bronze_trades`) with ingestion metadata.
4. Apply data-quality rules, splitting into valid rows and **Quarantine** (`quarantine_trades`).
5. Join valid rows to accounts/customers to build **Silver** (`silver_trades`).
6. Aggregate into **Gold** (`gold_daily_position`).
7. Print row-count and financial reconciliation results — both should say `True`.

### Step 7 — Validate
Open a **SQL Editor** (attach to a SQL warehouse, not the cluster) and run `sql/validation.sql` after replacing `<catalog>.<schema>` with your values (or run `USE CATALOG <catalog>; USE SCHEMA <schema>;` first). It checks:
- Row counts per layer
- Bronze = Silver + Quarantine reconciliation
- Quarantine reasons breakdown and quarantine rate
- Orphan account keys (should be 0)
- Duplicate `trade_id` in silver (should be 0 rows)
- Silver vs. Gold financial reconciliation
- Delta table history (time travel)

---

## 4. (Optional) Deploy as a scheduled job

`resources/job.yml` defines a **Databricks Asset Bundle** job that chains: ingest/validate → bronze→silver → SQL quality gate → silver→gold → notify, on a daily 06:00 UTC schedule (created **paused**).

1. Install the Databricks CLI (v0.200+) and configure auth: `databricks configure`.
2. Create a `databricks.yml` bundle file alongside `resources/` if one doesn't exist yet, referencing `resources/job.yml`.
3. Edit `resources/job.yml`:
   - Replace `warehouse_id: "<sql-warehouse-id>"` with a real SQL warehouse ID.
   - Replace `you@example.com` with your notification email.
   - Replace or remove the placeholder `run_job_task` (`job_id: 0`).
4. Deploy:
   ```bash
   databricks bundle deploy -t dev
   ```
5. Unpause the schedule in the Jobs UI when ready, or run it on demand with `databricks bundle run batch_etl_job -t dev`.

---

## 5. Run the unit tests locally (no Databricks required)

```bash
pip install pyspark==3.5.1 pytest
pytest tests/test_batch.py -v
```

These tests re-implement the pure transformation functions (`cast_types`, `apply_data_quality`) against tiny in-memory DataFrames and check: null/duplicate/invalid-side quarantine behavior, correct trade value calculation, row-count reconciliation, and idempotency on rerun.

---

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `CREATE CATALOG` fails with a permission error | Your user lacks metastore admin / `CREATE CATALOG` privilege — ask a workspace admin |
| Volume upload fails / path not found | Metastore not assigned to the workspace, or catalog/schema/volume creation cell wasn't run first |
| `quality_gate` SQL task fails in the bundle job | `warehouse_id` placeholder wasn't replaced with a real SQL warehouse ID |
| Financial reconciliation prints `False` | Check the quarantine reasons breakdown (query 3 in `validation.sql`) — usually a schema/type mismatch in the source CSV |
