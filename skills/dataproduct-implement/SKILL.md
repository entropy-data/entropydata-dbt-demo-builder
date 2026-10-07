---
name: dataproduct-implement
description: Given an Entropy Data data product URL or id, sync its ODPS and ODCS into `dags/dataproducts/<id>/` of the Stackable demo repo, generate Trino dbt models from the contract, run `dbt parse`, then get it running: either push to Forgejo and trigger the Airflow DAG (publish, `dbt-ol build` with OpenLineage, `datacontract test`), or run the same steps locally. Demo-grade. Trigger when the user asks to "implement the data product <url>", "build the dbt pipeline for this data product", or "scaffold dbt models from a data contract".
---

# Implement a data product from its data contract (demo)

Turn a data product into a working Trino dbt pipeline in the Stackable demo and prove it works, in one pass. The data contract (ODCS) is the source of truth for the output schema; this skill reads it, writes dbt artifacts that produce data matching the contract, and hands the folder to the generic Airflow DAG (or runs the same steps locally) so the data product shows up in Entropy Data with lineage and test results.

Everything lives in one folder, `dags/dataproducts/<id>/`. The DAG `dataproduct_<id with - replaced by _>` re-publishes the folder's ODPS and ODCS files to Entropy Data on every run, so **the files in the folder win** over what is in Entropy Data.

## When to use this vs. other skills

- **No folder for this data product yet** → run `dataproduct-bootstrap` first, then come back here. (If the data product was designed in the Entropy Data UI, run bootstrap with the same id; this skill then overwrites the seeded ODPS and ODCS with the published ones.)
- **Folder exists, want to derive Trino models from the contract and prove the pipeline works** → this skill.

## How to run this skill

> `${PLUGIN_ROOT}` below refers to the root of this plugin, the directory that contains `skills/`. On Claude Code it is set automatically as `${CLAUDE_PLUGIN_ROOT}`; use that. On any other agent (Codex, Copilot CLI, etc.) it is unset; resolve it as `../..` relative to **this `SKILL.md` file's directory** (i.e. the grandparent of `skills/<this-skill>/`).

### Plan announcement (before Step 0)

Before running Step 0, print this plan to the user verbatim:

> Running **dataproduct-implement**. I'll:
> 1. Pre-checks: confirm this is a demo repo checkout with a `dags/dataproducts/<id>/` folder; `dbt` (`dbt-trino`) and `entropy-data` are on PATH; the Entropy Data connection works.
> 2. Resolve the data product and sync its ODPS into the folder.
> 3. Sync each output port's data contract into the folder.
> 4. Translate the ODCS schema into dbt mart models: SQL column projections + `_marts.yml` tests.
> 5. Wire input ports from the ODPS as dbt sources, project columns 1:1, leave the rest as TODOs.
> 6. Run `dbt parse` to catch syntax errors.
> 7. Get it running: push to Forgejo and trigger the Airflow DAG, or run publish + `dbt-ol build` + `datacontract test` locally.
> 8. Summarize what was generated, what ran, and what's still TODO.

Then proceed.

### Step 0: Pre-checks

- Confirm `dags/dataproducts/` exists at the working directory root. If not, tell the user to clone the demo repo from Forgejo (`git clone http://<node-ip>:30000/stackable/entropydata-dbt-demo.git`) and stop.
- Confirm `dbt --version` (with the `trino` adapter listed), `entropy-data --version`, and `jq` are on PATH. If any are missing, surface the install line (`uv pip install dbt-core dbt-trino openlineage-dbt 'datacontract-cli[trino]' entropy-data`) and stop. `dbt-ol` and `datacontract` are only needed for the local run in Step 6.
- Confirm `entropy-data connection test` succeeds. If not, stop and tell the user to run `entropy-data connection add default --api-key <key> --host http://<node-ip>:30808`.

Always use the `entropy-data` CLI for any connection to Entropy Data (data products, data contracts, publishing). Do not use the Entropy Data MCP server.

### Step 1: Resolve the data product

Accept either a full URL (`http://<node-ip>:30808/myorga/dataproducts/<id>`, extract the trailing id) or a bare id. Derive `DATA_PRODUCT_DIR` = `dags/dataproducts/<id>` and `DAG_ID` = `dataproduct_<id with - replaced by _>`.

- If `DATA_PRODUCT_DIR` does not exist, route the user to `dataproduct-bootstrap` with this id and stop.
- Run `entropy-data dataproducts get <id> -o yaml`. If it exists in Entropy Data and differs from `<DATA_PRODUCT_DIR>/<id>.odps.yaml`, surface the diff and ask which version wins. If the user picks Entropy Data, write it to the local file.
- If it does not exist in Entropy Data yet, use the local ODPS file. The DAG will publish it.

Remember the result as `DATA_PRODUCT`. Extract:

- `DATA_PRODUCT_ID`, `DATA_PRODUCT_NAME`, owning team, purpose
- `inputPorts` (each has a `name`, usually the upstream data product id, and a `contractId`)
- `outputPorts` (each has a `name` and a `contractId`)

If the data product has more than one output port, ask the user which one(s) to implement. Default to all.

Make sure the top-level `customProperties` of the local ODPS contains `property: "dataProductBuilder"` with `value: "https://github.com/entropy-data/entropydata-dbt-demo-builder"`. Add it if missing.

### Step 2: Sync the data contract

For each selected output port, look for `<DATA_PRODUCT_DIR>/<contract-id>.odcs.yaml` and run `entropy-data datacontracts get <contract-id> -o yaml`. Same rule as Step 1: if both exist and differ, surface the diff and ask which wins; if only one exists, use it. Remember the result as `CONTRACT`.

You need from `CONTRACT`:

- `schema` (table name → properties list with `logicalType`, `physicalType`, `required`, `primaryKey`, `unique`, `description`, `classification`)
- `servers`: expect one entry `server: trino`, `type: trino`, `host: trino-coordinator`, `port: 8443`, `catalog: hive-iceberg`, `schema: <schema>`. The schema must match `schema:` in `<DATA_PRODUCT_DIR>/profiles.yml`; if they disagree, ask which one to fix.

### Step 3: Translate ODCS schema to dbt artifacts

**Output column identifier rule (applies to every property in this step and Step 4).** For every contract property, resolve `OUT_COL = property.physicalName // property.name`, preferring `physicalName` when present. Use `OUT_COL` for the SQL alias **and** the `_marts.yml` `columns: - name:` entry so the projected column, the dbt tests, and the materialized Trino column all agree with the contract. Trino folds unquoted identifiers to lowercase; if `OUT_COL` is not all-lowercase, double-quote it in the SQL alias (`as "MixedCase"`) and flag it to the user, since Iceberg tables in Trino store lowercase column names anyway.

For each table in each contract:

1. Decide the dbt model name. Default: the table's `physicalName // name`.
2. Generate `<DATA_PRODUCT_DIR>/models/marts/<table>.sql`: a stub `select` that projects each contract column with `cast(... as <trino-type>) as <OUT_COL>`. Leave the `from` clause as a TODO commented with the candidate input ports from the ODPS. Prepend a one-line header comment:

   ```sql
   -- Governed by <contract-id>.odcs.yaml (ODCS id: <CONTRACT_ID>)
   ```

3. Append the model to `<DATA_PRODUCT_DIR>/models/marts/_marts.yml`:

   ```yaml
   models:
     - name: <table>
       description: <from contract>
       meta:
         data_contract:
           id: <CONTRACT_ID>
           file: <contract-id>.odcs.yaml
       columns:
         - name: <col>
           description: <from contract>
           data_tests: [not_null]   # only when required or primaryKey
   ```

   Translate ODCS rules to dbt tests: `required: true` → `not_null`, `unique: true` or `primaryKey: true` → `unique` (+ `not_null`), enum → `accepted_values`.

4. Map ODCS types to Trino types. If the property has a `physicalType`, use it as is (e.g. `bigint`, `varchar`, `double`). Otherwise map `logicalType`:

| ODCS `logicalType` | Trino |
|---|---|
| `string`/`text` | `varchar` |
| `integer`/`long` | `bigint` |
| `number`/`decimal`/`numeric` | `double` (or `decimal(p,s)` if the contract states precision) |
| `boolean` | `boolean` |
| `timestamp` | `timestamp(6)` |
| `date` | `date` |

### Step 4: Wire input ports and project columns

For each output port:

1. **Declare each input port as a dbt source.** For every entry in `DATA_PRODUCT.inputPorts`:
   1. Load the upstream contract: `dags/dataproducts/<input-port-name>/<contract-id>.odcs.yaml` if it is in the repo, else `entropy-data datacontracts get <contract-id> -o yaml`.
   2. Take `servers[0].catalog` and `servers[0].schema` (e.g. `tpch` / `tiny` for `tpch-source`, `hive-iceberg` / `demo` for `tpch-core` and the marts).
   3. Write or complete the source in `<DATA_PRODUCT_DIR>/models/staging/_sources.yml`:

      ```yaml
      version: 2
      sources:
        - name: <input-port-name with - replaced by _>
          database: <catalog>
          schema: <schema>
          meta:
            data_contract:
              id: <contract-id>
          tables:
            - name: <table>
              description: <from contract>
              columns:
                - name: <col>
                  description: <from contract>
      ```

   Do not copy upstream contracts into the folder: the DAG publishes every ODCS file in the folder and would overwrite the upstream owner's contract. Surface diffs and ask before overwriting an existing `_sources.yml` entry. `dags/dataproducts/tpch-core/models/staging/_sources.yml` (reads `tpch-source`) and `dags/dataproducts/tpch-order-summary/models/_sources.yml` (reads `tpch-core`) are working examples.

2. **Match input columns to output columns** by name (comparing against both the input property's `name` and its `physicalName`), or by an obvious synonym only if the input contract's description makes it explicit. For each output column, if exactly one input has a matching column, project `cast(<input_col> as <trino_type>) as <OUT_COL>`. Otherwise leave `cast(null as <trino_type>) as <OUT_COL>` with a `-- TODO:` comment.

3. **Write the SQL body.**
   - **Single input table, columns match 1:1** → replace the TODO `from` with `from {{ source('<source-name>', '<table>') }}`.
   - **Multiple input tables** → leave the join logic as an inline TODO listing each candidate `{{ source(...) }}` and the join keys. If the user wants the joins written, add `stg_<table>.sql` views under `models/staging/` (see `tpch-core` for the pattern) and join them in the mart.
   - **Derived / aggregated columns** → leave as `cast(null as <trino_type>) as <col>` with a `-- TODO: compute <description>` comment.

### Step 5: `dbt parse`

From `DATA_PRODUCT_DIR`, run:

```
TRINO_PASSWORD=${TRINO_PASSWORD:-unused} dbt parse --profiles-dir .
```

`dbt parse` does not connect to Trino, but the profile requires `TRINO_PASSWORD` to be set. If it fails, surface the error, fix obvious mistakes (wrong source name, typos in `_marts.yml`), and re-run. Do not proceed to Step 6 with a failing parse. Delete the `target/` and `logs/` directories dbt created (they are gitignored, but keep the folder clean).

### Step 6: Get it running

Ask the user: "Run it in Airflow (push to Forgejo, then trigger the DAG `<DAG_ID>`) or locally now?" Default to Airflow; it is the demo path and needs no local Trino access.

If any mart model still has TODOs (unwired `from`, derived columns, joins), warn the user that the build will fail those models.

**Option A: Airflow (default).**

1. Show the commands. Run them only if the user explicitly asks you to:

   ```
   git add dags/dataproducts/<id>
   git commit -m "Implement data product <id>"
   git push
   ```

   The remote is `http://<node-ip>:30000/stackable/entropydata-dbt-demo.git`, branch `main` (user `stackable` / `stackable`).
2. Airflow git-syncs `dags/` within a minute or two. The user triggers `<DAG_ID>` in the Airflow UI (`admin` / `admin`). The DAG publishes the ODPS and all ODCS files, runs `dbt-ol build --profiles-dir .` with OpenLineage pointed at `api/v1/lineage?dataProductId=<id>&outputPortId=<first output port name>`, and runs `datacontract test <file> --server trino --publish <entropy-data>/api/test-results` per output-port contract.
3. Mark the run steps `skipped (Airflow)` in the report; the user reads the results in Airflow and Entropy Data.

**Option B: Local.** Trino must be reachable as `trino-coordinator:8443`: `kubectl port-forward svc/trino-coordinator 8443:8443` plus a `127.0.0.1 trino-coordinator` line in `/etc/hosts` (keeps the TLS hostname valid). Require `TRINO_PASSWORD` in the environment and, if the Trino certificate is not in the system trust store, `TRINO_CA_BUNDLE` pointing at the CA file. Do not source credentials yourself. Confirm `dbt-ol --version` and `datacontract --version` are on PATH. Then run from `DATA_PRODUCT_DIR`, mirroring the DAG:

1. Publish:

   ```
   entropy-data dataproducts put <id> --file <id>.odps.yaml
   entropy-data datacontracts put <contract-id> --file <contract-id>.odcs.yaml   # per output-port contract
   ```

2. Build and ship lineage. Confirm first: "Run `dbt-ol build` against Trino now? This materializes the models in `hive-iceberg.<schema>`, runs the dbt tests, and ships the lineage event to Entropy Data." Wait for explicit yes.

   ```
   API_HOST=$(entropy-data connection get -o json | jq -r .host)
   OPENLINEAGE__TRANSPORT__TYPE=http \
   OPENLINEAGE__TRANSPORT__URL=$API_HOST \
   OPENLINEAGE__TRANSPORT__ENDPOINT="api/v1/lineage?dataProductId=<id>&outputPortId=<first-output-port-name>" \
   OPENLINEAGE__TRANSPORT__AUTH__TYPE=api_key \
   OPENLINEAGE__TRANSPORT__AUTH__APIKEY=$(entropy-data connection get -o json | jq -r .apiKey) \
     dbt-ol build --profiles-dir .
   ```

   Non-zero exit means at least one model or test failed; surface the dbt log section, do not retry silently.

3. Data contract test, per output-port contract:

   ```
   DATACONTRACT_TRINO_USERNAME=${TRINO_USER:-admin} \
   DATACONTRACT_TRINO_PASSWORD=$TRINO_PASSWORD \
   ENTROPY_DATA_API_KEY=$(entropy-data connection get -o json | jq -r .apiKey) \
     datacontract test <contract-id>.odcs.yaml --server trino --logs --publish $API_HOST/api/test-results
   ```

Do not echo or persist API keys or passwords; export them only as part of the command that needs them. If the build succeeded, **the data product is now visible in Entropy Data with materialized tables, lineage, and test results.** Tell the user this explicitly in the final report; it is the whole point of the demo.

### Step 7: Final report

End with this two-part recap. Use the `Status` enum: `created`, `updated`, `already present`, `passed`, `failed`, `skipped`.

**Part 1: outcome table.**

| Artifact | Status | Details |
|---|---|---|
| Data product ODPS | … | `<DATA_PRODUCT_DIR>/<id>.odps.yaml` (source: local / Entropy Data) |
| `dataProductBuilder` customProperty | … | "added" / "already present" |
| Output-port data contract `<CONTRACT_ID>` | … | `<DATA_PRODUCT_DIR>/<contract-id>.odcs.yaml` |
| Input-port sources | … | `<N>` sources in `models/staging/_sources.yml` |
| Model `<table>.sql` | … | "wired to `<source>`" / "join TODO" / "skipped per user" |
| `_marts.yml` entries | … | counts |
| `dbt parse` | … | passed / failed: `<reason>` |
| Publish ODPS + ODCS | … | "skipped (Airflow)" / "passed" / "failed" |
| `dbt-ol build` | … | "skipped (Airflow)" / "passed: N models, M tests, lineage shipped" / "failed" |
| `datacontract test` | … | "skipped (Airflow)" / per contract: "passed" / "failed: <count>" |

**Part 2: next steps.** Bullet list, only what applies:

- For each model with a join or derived-column TODO, name the inputs and the missing logic.
- For each `failed` row, the concrete next action (which model, which test, which contract rule).
- For the Airflow path: the push commands (if not run yet) and "trigger `<DAG_ID>` in the Airflow UI".
- Link the user to `http://<node-ip>:30808/myorga/dataproducts/<id>` to see the data product, lineage, and test results land.

If everything passed and there are no TODOs, write: `Pipeline implemented, materialized, tested, and lineage published. Nothing else to do.`

## Constraints

- **Trino on Stackable only.** The type map, profile, and CLI invocations target the demo's Trino. If `profiles.yml` is not `type: trino`, stop.
- **Contract is source of truth for schema, not logic.** Generate columns, types, and tests from the contract. Project and cast in SQL only; do not invent joins, aggregations, or column derivations.
- **No secrets in the folder.** Airflow git-syncs it; credentials come from env vars only.
- **Don't overwrite existing dbt SQL silently.** Surface the diff and ask.
- **Don't auto-fix failing dbt or datacontract tests.** Report them; the fix belongs to the user.
- **Don't commit or push** unless the user explicitly asks.
- **Idempotent.** Re-running on the same data product when files already match the contract is a no-op (except the run/test commands, which always re-execute).
