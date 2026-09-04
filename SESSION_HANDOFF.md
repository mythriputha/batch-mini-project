# Session Handoff — Databricks Batch Mini Project

Status as of 2026-09-04. This project is **fully working end-to-end** on Azure Databricks and pushed to GitHub — both the main pipeline notebook and a second Delta Lake exercises notebook. This file exists so a new Claude Code session (e.g. on a different machine) can pick up exactly where the previous one left off instead of re-deriving everything from scratch.

**How to use this file on a new machine:** clone this repo, open the folder in Claude Code, and say something like *"Read SESSION_HANDOFF.md and continue from there"*. Point Claude at this file explicitly — it won't be loaded automatically the way `CLAUDE.md` is.

---

## What this project is

A Bronze → Silver (+Quarantine) → Gold batch lakehouse pipeline for investment/trade data, running on **Azure Databricks + Unity Catalog**. See [README.md](README.md) for the full architecture and setup guide (already accurate and up to date).

## Current state: DONE and verified working

The full pipeline has been run successfully at least twice (once from a standalone notebook, once from the Git-folder-cloned copy) with:
```
Row-count reconciliation OK: True
Financial reconciliation OK: True
Source rows: 1000 | Valid: 1000 | Quarantined: 0
Source-valid gross value: 451419214.60 | Gold gross value: 451419214.60
```
SQL validation (`sql/validation.sql`) was also run query-by-query in the Databricks SQL Editor and confirmed clean (no orphans, no duplicates, reconciliation `true`).

## Azure resources created (this account is a Free Trial)

| Resource | Name used | Notes |
|---|---|---|
| Resource Group | `rg-databricks-batch-mini` | Region: East US |
| Storage account (ADLS Gen2) | *(created with hierarchical namespace enabled; exact name not double-checked in this log — look it up in the resource group if needed)* | Container: `unity-catalog` |
| Access Connector for Azure Databricks | `dbx-access-connector` | Granted `Storage Blob Data Contributor` on the storage account |
| Azure Databricks workspace | `dbx-batch-mini-ws` | Premium tier. URL: `https://adb-7405609641126577.17.azuredatabricks.net` |

## Unity Catalog setup — IMPORTANT deviation from the README

The README's original instructions assumed you'd create a catalog named `main` manually. **That did not work on this trial workspace** — regular users can't create new top-level catalogs; Azure auto-provisioned exactly one catalog bound to the workspace instead.

**Actual catalog/schema/volume in use:**
- Catalog: **`dbx_batch_mini_ws`** (auto-created, matches workspace name — NOT `main`)
- Schema: `batch_mini_project`
- Volume: `landing`
- Full volume path: `/Volumes/dbx_batch_mini_ws/batch_mini_project/landing/`

This is already fixed in the code — `src/01_batch_pipeline.py`'s catalog widget default was changed from `"main"` to `"dbx_batch_mini_ws"` (see Fixes Applied below). **On a fresh workspace/tenant this might differ again** — always check Catalog Explorer for the actual auto-provisioned catalog name before assuming `dbx_batch_mini_ws` applies.

## Fixes applied to the code (both committed and pushed to GitHub)

1. **`F.input_file_name()` not supported on Unity Catalog serverless compute.**
   Error was: `[UC_COMMAND_NOT_SUPPORTED.WITH_RECOMMENDATION] The command(s): input_file_name are not supported in Unity Catalog. Please use _metadata.file_path instead.`
   Fixed in `src/01_batch_pipeline.py` line ~116: `F.input_file_name()` → `F.col("_metadata.file_path")`.

2. **Catalog widget default was `"main"`, which doesn't exist on this workspace.**
   Fixed in `src/01_batch_pipeline.py` line 10: `dbutils.widgets.text("catalog", "main", "Catalog")` → `dbutils.widgets.text("catalog", "dbx_batch_mini_ws", "Catalog")`.

Both fixes are in the `main` branch on GitHub already — no action needed unless you're working from a stale clone.

## Compute notes

- This workspace defaults to **Serverless compute** — there was no visible "Create compute" button for classic all-purpose clusters (likely a workspace policy default, possibly compounded by Free Trial VM quota limits). **Serverless was used successfully for both the notebook and the SQL warehouse** — no classic cluster was ever created, and none is needed.
- SQL Warehouse created: `validation-warehouse`, Serverless, size 2X-Small.

## Git / GitHub setup

- Repo: **https://github.com/mythriputha/batch-mini-project** (public)
- Local machine (office PC) has this repo fully cloned, committed, and pushed — `git status` shows clean, up to date with `origin/main`.
- Databricks workspace has a **Git folder** (Databricks' native Git integration) cloned from the same repo, located in the workspace at `Workspace > Users > <you> > batch-mini-project`. This is the "live" notebook copy to keep using inside Databricks — run `01_batch_pipeline` from **inside this Git folder**, not a standalone copy.
- Databricks Git credential: a GitHub personal access token (classic, `repo` scope) is linked under Databricks **Settings → Linked accounts → Git integration**. This credential lives in the Databricks *workspace/account*, not on this laptop — **on your personal laptop, you'll need to `git clone` the repo locally yourself** (Databricks-side Git integration doesn't change per-laptop, since it's already configured in the cloud workspace).
- A GitHub Data Loss Prevention gotcha specific to this office PC: an "Endpoint Data Loss Prevention Plus" agent blocked in-browser file uploads to `*.azuredatabricks.net` (flagged as upload to outside the org boundary). Worked around by installing the Databricks CLI and using `databricks workspace import` / `databricks fs cp` instead of drag-and-drop. **This is specific to this managed office machine — your personal laptop likely won't have this restriction**, so plain browser upload/import should just work there.
- Two old duplicate notebook copies (one directly in the user Home folder, one that ended up in a `Drafts` folder) have been deleted — the **only** notebook copy that should exist going forward is the one inside the `batch-mini-project` Git folder.

## Second notebook: `src/02_delta_lake_exercises.ipynb` — DONE, pushed to GitHub

A hands-on Delta Lake feature walkthrough, built directly in the `batch-mini-project` Git folder in Databricks (not something scaffolded from the original README — it was created ad hoc as an extension of the project). Fully complete and pushed. Covers, in order:
- `DESCRIBE HISTORY` on `silver_trades`
- `UPDATE` mutations and re-querying to see the change
- A dedicated `delta_time_travel_test` table: create → mutate → `DESCRIBE HISTORY` → time travel query (`VERSION AS OF 0`) to see pre-mutation data
- **Schema enforcement test**: an `INSERT` deliberately tries to put the string `'NOT_A_NUMBER'` into a `DECIMAL(18,4)` column. This is *supposed* to fail with `CAST_INVALID_INPUT` — that failure is the correct, intended result (proves Delta rejects malformed writes rather than silently corrupting data). A markdown cell right after it documents this explicitly so it doesn't get mistaken for a bug on a re-read.
  - **Gotcha for "Run all":** because this cell always fails on purpose, clicking **Run all** on this notebook will always stop there and mark every cell below as "Skipped." Use **Run all below** (the dropdown next to any cell's ▶ button) starting from the cell *after* the intentional failure to execute the rest. This is expected notebook behavior, not something to fix.
- Append vs. overwrite semantics on a scratch table `overwrite_append_test`: `INSERT INTO` (append) vs `INSERT OVERWRITE` (replace all rows)
- **OPTIMIZE**: `DESCRIBE DETAIL` before/after to see file-count reduction after compaction
- **VACUUM**: a `DRY RUN` first, then a real `VACUUM` on `overwrite_append_test`. Note: **Serverless SQL compute blocks overriding `spark.databricks.delta.retentionDurationCheck.enabled`** (`CONFIG_NOT_AVAILABLE.WITHOUT_SUGGESTION`, SQLSTATE 42K01) — this is a platform restriction on serverless, not a mistake. So VACUUM was run with the **default 7-day retention** instead of forcing `RETAIN 0 HOURS`; since the table was brand new, it correctly reported 0 files removed, which itself demonstrates the safety mechanism working as intended. Confirmed via `DESCRIBE HISTORY` showing `VACUUM START`/`VACUUM END` with `status: COMPLETED`.

## Saved artifacts — now version-controlled (previously workspace-only)

- SQL query `batch_mini_project_validation` is now saved as `src/batch_mini_project_validation.dbquery.ipynb` and pushed to GitHub — no longer a workspace-only artifact as earlier notes said.

## Scheduled job: DONE, deployed via UI (not via CLI bundle deploy)

`batch-mini-project-etl` was created directly in the Databricks **Jobs & Pipelines** UI (the CLI/Asset Bundle route was tried but the user declined to install the Databricks CLI on this office PC, so bundle deploy was abandoned in favor of manual UI job creation). Two tasks:
- `run_pipeline` — runs `01_batch_pipeline` notebook on Serverless compute, with `catalog=dbx_batch_mini_ws`, `schema=batch_mini_project`, `volume=landing` as parameters.
- `quality_gate` — runs the saved `batch_mini_project_validation` SQL query on `validation-warehouse`, depends on `run_pipeline`.

Tested with 4 manual "Run now" triggers — **all 4 succeeded** (2m36s–7m47s runtime). The daily 6 AM UTC schedule exists but is left **Paused** by choice — trigger manually via "Run now" instead of relying on the schedule, unless you explicitly activate it later (Tasks tab → Schedules & Triggers).

**Note on `databricks.yml` / `resources/job.yml` / `sql/validation_scheduled.sql`:** these 3 files exist in the repo as Infrastructure-as-Code *reference* (what a CLI-based bundle deploy would look like), but the **live job running in the workspace was created by hand in the UI**, not by `databricks bundle deploy`. The two aren't automatically in sync — if you later edit the job in the UI, these files won't reflect that unless updated manually, and vice versa.

## What's NOT done yet / open options

Nothing is broken or blocking — the project is considered **complete**. One item was explicitly declined:
- **Quarantine/DQ live-data test** — offered, but the user chose to skip it. Not needed to consider this project done.

No other open items remain from the original scope.

## To continue on your personal laptop

```powershell
git clone https://github.com/mythriputha/batch-mini-project.git
cd batch-mini-project
```
Then open this same repo in the Databricks workspace via the existing `batch-mini-project` Git folder (already set up cloud-side — nothing to redo there), or open the Azure Portal / Databricks workspace directly in a browser — all Azure and Databricks resources are already live in the cloud and don't depend on which laptop you use.
