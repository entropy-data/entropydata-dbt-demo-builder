---
name: dataproduct-bootstrap
description: Bootstrap a brand-new data product folder in the Stackable + Entropy Data demo repo in one go: `dags/dataproducts/<id>/` with ODPS, output-port ODCS, and a Trino dbt project (dbt_project.yml, profiles.yml, sources + marts model layout, README). If the data product already exists in Entropy Data (designed in the web UI), starts from its ODPS and output-port ODCS. The generic Airflow DAG picks the folder up after a push to Forgejo. Trigger when the user asks to start a new data product, scaffold a new dbt project in the demo, or "create a data product from scratch."
---

# Bootstrap a new data product folder (demo)

Scaffold a Trino dbt data product as one folder in the `entropydata-dbt-demo` repo, following the demo's conventions. Demo-grade: one batched question, one platform, no audits, no migration logic. No CI workflow and no OpenLineage config: the generic Airflow DAG (`dags/dataproduct_dags.py`) publishes, builds, ships lineage, and tests every folder under `dags/dataproducts/`.

For populating the contract schema and dbt model bodies, use the **dataproduct-implement** skill after this one.

## What this skill produces

After running, the demo repo contains:

```
dags/dataproducts/<data-product-id>/
├── <data-product-id>.odps.yaml
├── <contract-id>.odcs.yaml
├── dbt_project.yml
├── profiles.yml
├── README.md
├── .gitignore
└── models/
    ├── _sources.yml
    └── marts/
        └── _marts.yml
```

The existing data products in the demo are good references: `dags/dataproducts/tpch-source/` (source-aligned, ODPS + ODCS only, no dbt), `dags/dataproducts/tpch-core/` (dbt project with the cleaned staging views `stg_*` in `"hive-iceberg".demo`, input port from `tpch-source`) and six mart data products such as `dags/dataproducts/tpch-order-summary/` (dbt project with one mart table that reads `tpch-core` via `source('tpch_core', ...)` directly, no staging layer of its own; one output port and one data contract).

## How to run this skill

> `${PLUGIN_ROOT}` below refers to the root of this plugin, the directory that contains `skills/`. On Claude Code it is set automatically as `${CLAUDE_PLUGIN_ROOT}`; use that. On any other agent (Codex, Copilot CLI, etc.) it is unset; resolve it as `../..` relative to **this `SKILL.md` file's directory** (i.e. the grandparent of `skills/<this-skill>/`).

### Plan announcement (before Step 1)

Before running Step 1, print this plan to the user verbatim:

> Running **dataproduct-bootstrap**. I'll:
> 1. Pre-check: confirm this is a checkout of the demo repo and the Entropy Data connection works.
> 2. If the data product already exists in Entropy Data, start from its ODPS and data contract; otherwise ask you for parameters in one batched question (id, name, purpose, team, input data product, schema, table).
> 3. Scaffold `dags/dataproducts/<id>/`: ODPS, output-port ODCS, dbt_project.yml, profiles.yml, model layout, README, .gitignore.
> 4. Summarize what was scaffolded and how to get it running in Airflow.

Then proceed.

### Step 1: Pre-checks

- Confirm the working directory is the root of a demo repo checkout: `dags/dataproducts/` exists. If not, stop and tell the user to clone it from Forgejo: `git clone http://<node-ip>:30000/stackable/entropydata-dbt-demo.git` (user `stackable` / `stackable`).
- Confirm `entropy-data connection test` succeeds. If not, stop and tell the user to create an API key in Entropy Data (`http://<node-ip>:30808`, log in as `demo-admin`, **Organization Settings → API Keys**) and run `entropy-data connection add default --api-key <key> --host http://<node-ip>:30808`.

Always use the `entropy-data` CLI for any connection to Entropy Data. Do not use the Entropy Data MCP server.

### Step 2: Gather parameters in one batched question

If the user already named an id (or `dataproduct-implement` sent you here), run `entropy-data dataproducts get <id> -o yaml` first. If it succeeds, the data product was designed in Entropy Data: fetch its first output port's contract with `entropy-data datacontracts get <contractId> -o yaml` and derive every parameter you can from the two documents (`EXISTING = true`):

- `DATA_PRODUCT_NAME`, `PURPOSE` (`description.purpose`), `TEAM_NAME` from the ODPS
- `OUTPUT_PORT_NAME`, `CONTRACT_ID` from `outputPorts[0]`
- `TABLE` from the contract's `schema[0].physicalName // name`
- `SCHEMA` from the contract's `servers[].schema` (the `trino` server)

Only ask for what is still missing (typically `INPUT_DATA_PRODUCT_ID`, sometimes `SCHEMA`).

Otherwise run `entropy-data teams list` so you can offer the existing team ids. Then ask the user for these in a single prompt. Do not generate any files until you have all of them.

| Parameter | Description | Example |
|---|---|---|
| `DATA_PRODUCT_ID` | Stable id, kebab-case, also the folder name | `customer-orders` |
| `DATA_PRODUCT_NAME` | Human-friendly name | `Customer Orders` |
| `PURPOSE` | One sentence: why this data product exists | `Order history per customer for customer success.` |
| `TEAM_NAME` | Owning team id, must exist in Entropy Data | `analytics-engineering` |
| `INPUT_DATA_PRODUCT_ID` | Upstream data product to read from (default `tpch-core`) | `tpch-core`, `tpch-source` or a mart such as `tpch-order-summary` |
| `SCHEMA` | Iceberg schema in the `hive-iceberg` catalog (default: `DBT_PROJECT_NAME`) | `customer_orders` |
| `TABLE` | First output-port table name | `customer_orders` |

If `dags/dataproducts/<DATA_PRODUCT_ID>/` already exists, stop: this skill only handles new folders. Point the user at `dataproduct-implement` to add models to an existing data product.

Derive:

- `DATA_PRODUCT_DIR` = `dags/dataproducts/<DATA_PRODUCT_ID>`
- `DBT_PROJECT_NAME` = `DATA_PRODUCT_ID` with `-` replaced by `_` (dbt project and profile name)
- `DAG_ID` = `dataproduct_<DBT_PROJECT_NAME>`
- `OUTPUT_PORT_NAME` = `<TABLE>` with `_` replaced by `-` (one output port per data product, like the TPC-H marts), unless taken from Entropy Data
- `CONTRACT_ID` = `<DATA_PRODUCT_ID>` (the output port's data contract shares the data product's id), unless taken from Entropy Data
- `CONTRACT_FILE` = `<CONTRACT_ID>.odcs.yaml`
- `DATA_PRODUCT_NAME_URLENCODED` = `DATA_PRODUCT_NAME` URL-encoded (`Customer%20Orders`), for the Superset Open In link in the ODPS
- `ODPS_FILE` = `<DATA_PRODUCT_ID>.odps.yaml`
- `INPUT_SOURCE_NAME` = `INPUT_DATA_PRODUCT_ID` with `-` replaced by `_`
- `INPUT_CONTRACT_ID`, `INPUT_CATALOG`, `INPUT_SCHEMA` from the upstream data product: read `dags/dataproducts/<INPUT_DATA_PRODUCT_ID>/<INPUT_DATA_PRODUCT_ID>.odps.yaml` → first output port's `contractId`, then that contract's `servers[0].catalog` / `servers[0].schema` (`tpch-source` → `tpch-source`, `tpch` / `tiny`; `tpch-core` → `tpch-core`, `hive-iceberg` / `demo`). If the upstream folder is not in the repo, use `entropy-data dataproducts get <id> -o yaml` and `entropy-data datacontracts get <contract-id> -o yaml` instead.

### Step 3: Scaffold the folder

Templates live under `${PLUGIN_ROOT}/skills/dataproduct-bootstrap/templates/`. Copy each one into `DATA_PRODUCT_DIR`, substituting the `{{UPPER_CASE}}` placeholders. Leave dbt Jinja such as `{{ env_var('TRINO_PASSWORD') }}` and the `${SUPERSET_URL}` placeholder in the ODPS untouched (the DAG fills it in when publishing).

| Template | Destination (under `DATA_PRODUCT_DIR`) |
|---|---|
| `data-product.odps.yaml` | `<ODPS_FILE>` |
| `contract.odcs.yaml` | `<CONTRACT_FILE>` |
| `dbt_project.yml` | `dbt_project.yml` |
| `profiles.yml` | `profiles.yml` |
| `README.md` | `README.md` |
| `.gitignore` | `.gitignore` |
| `models/_sources.yml` | `models/_sources.yml` |
| `models/marts/_marts.yml` | `models/marts/_marts.yml` |

If `EXISTING = true`, do not seed the ODPS and ODCS from the templates. Write the fetched Entropy Data versions to `<ODPS_FILE>` and `<CONTRACT_FILE>` instead and only add what is missing:

- ODPS: the `dataProductBuilder` entry in `customProperties`, the `inputPorts` entry for `INPUT_DATA_PRODUCT_ID` (`name`, `version`, `contractId`), an output port `description` (`Table "hive-iceberg".<SCHEMA>.<TABLE>, built by dbt.`) and the tags `consumer-aligned`, `dbt`.
- ODCS: a `servers` entry `trino` (as in the template) if there is none.

Keep every field authored in Entropy Data as it is. After writing any ODCS file, run `datacontract lint <file>` yourself (the lint hook only fires for Edit/Write tool calls of an installed plugin) and fix lint errors. If `datacontract` is not on PATH yet, lint after dataproduct-implement Step 5 creates the venv.

### Step 4: Final report

End with this two-part recap. Use the `Status` enum: `created`, `already present`, `skipped`.

**Part 1: outcome table.**

| Artifact | Status | Details |
|---|---|---|
| Folder | … | `<DATA_PRODUCT_DIR>` |
| ODPS | … | `<ODPS_FILE>` (from Entropy Data / template), team `<TEAM_NAME>`, input port `<INPUT_DATA_PRODUCT_ID>` |
| Output-port contract | … | `<CONTRACT_FILE>` (from Entropy Data, or seeded with `id` + `updated_at`; server `trino`) |
| `dbt_project.yml` | … | profile = `<DBT_PROJECT_NAME>` |
| `profiles.yml` | … | Trino, `hive-iceberg.<SCHEMA>`, credentials from env vars |
| Model layout | … | `models/_sources.yml`, `models/marts/` |
| `README.md` | … | written |
| `.gitignore` | … | written |

**Part 2: next steps.** Bullet list:

- Run **dataproduct-implement** next to fill the contract schema and generate the dbt models.
- Commit and push to Forgejo: `git add <DATA_PRODUCT_DIR> && git commit -m "Add data product <DATA_PRODUCT_ID>" && git push` (remote `http://<node-ip>:30000/stackable/entropydata-dbt-demo.git`, branch `main`).
- Airflow git-syncs `dags/` within a minute or two. Trigger the DAG `<DAG_ID>` in the Airflow UI. It publishes the ODPS and ODCS to Entropy Data, runs `dbt-ol build` (lineage shows up in Entropy Data), and runs the data contract test.
- Open `http://<node-ip>:30808/myorga/dataproducts/<DATA_PRODUCT_ID>` to see the data product.

## Constraints

- **Trino on Stackable only.** The templates target the demo's Trino (`trino-coordinator:8443`, catalog `hive-iceberg`). For Snowflake, Databricks, BigQuery or Postgres, point the user at https://github.com/entropy-data/dataproduct-builder-dbt.
- **New folders only.** If `DATA_PRODUCT_DIR` exists, stop.
- **Do not run `dbt init`.** Use the templates here.
- **No secrets in the folder.** `profiles.yml` is committed and reads every credential from env vars; never write a password or API key into any file.
- **Do not commit or push** unless the user explicitly asks; surface the commands as next steps.
- **Idempotent.** Re-running for an existing data product id is a no-op (refuse if anything would be overwritten).
