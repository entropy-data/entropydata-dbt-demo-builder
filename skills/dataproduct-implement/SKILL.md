---
name: dataproduct-implement
description: Given an Entropy Data data product URL or id, merge its ODPS and ODCS into `dags/dataproducts/<id>/` of the Stackable demo repo, wire and request access to its input ports, implement the Trino dbt models the contract describes, run `dbt parse`, then get it running: either push to Forgejo and trigger the Airflow DAG (publish, `dbt-ol build` with OpenLineage, `datacontract test`), or run the same steps locally, and publish the result. Demo-grade. Trigger when the user asks to "implement the data product <url>", "implement the data product <id> with its data contract", "build the dbt pipeline for this data product", or "scaffold dbt models from a data contract".
---

# Implement a data product from its data contract (demo)

Turn a data product into a working, tested Trino dbt pipeline in the Stackable demo, in one pass. The data contract (ODCS) is the source of truth for the output schema and, through its descriptions, for the logic; this skill reads it, writes dbt artifacts that produce data matching the contract, and hands the folder to the generic Airflow DAG (or runs the same steps locally) so the data product shows up in Entropy Data with lineage and test results.

Everything lives in one folder, `dags/dataproducts/<id>/`. The DAG `dataproduct_<id with - replaced by _>` re-publishes the folder's ODPS and ODCS files to Entropy Data on every run, so the folder must hold the **merged** result (see Step 1).

## When to use this vs. other skills

- **No folder for this data product yet** (typical demo flow: the data product and its contract were created in the Entropy Data web UI) → this skill runs `dataproduct-bootstrap` with the same id first, which starts from the Entropy Data versions, then continues here.
- **Folder exists, want to derive Trino models from the contract and prove the pipeline works** → this skill.

## How to run this skill

> `${PLUGIN_ROOT}` below refers to the root of this plugin, the directory that contains `skills/`. On Claude Code it is set automatically as `${CLAUDE_PLUGIN_ROOT}`; use that. On any other agent (Codex, Copilot CLI, etc.) it is unset; resolve it as `../..` relative to **this `SKILL.md` file's directory** (i.e. the grandparent of `skills/<this-skill>/`).

### Plan announcement (before Step 0)

Before running Step 0, print this plan to the user verbatim:

> Running **dataproduct-implement**. I'll:
> 1. Pre-checks: confirm this is a demo repo checkout; `entropy-data`, `uv` and `jq` are on PATH; the Entropy Data connection works.
> 2. Resolve the data product (bootstrapping the folder if needed) and merge its ODPS with Entropy Data.
> 3. Merge each output port's data contract into the folder.
> 4. Choose the input data products, add them as input ports and dbt sources, and request access to them.
> 5. Implement the dbt mart models the contract describes: SQL + `_marts.yml` tests.
> 6. Set up a Python venv in the folder and run `dbt parse`.
> 7. Get it running: push to Forgejo and trigger the Airflow DAG, or run `dbt-ol build` + `datacontract test` locally.
> 8. Publish ODPS and ODCS, set the status to active if all tests pass, and summarize.

Then proceed.

### Step 0: Pre-checks

- Confirm `dags/dataproducts/` exists at the working directory root. If not, tell the user to clone the demo repo from Forgejo (`git clone http://<node-ip>:30000/stackable/entropydata-dbt-demo.git`) and stop.
- Confirm `entropy-data --version`, `uv --version` and `jq --version` succeed. If any are missing, surface the install line (`uv tool install entropy-data`, https://docs.astral.sh/uv/) and stop. dbt, dbt-ol and the Data Contract CLI come from a venv in Step 5, not from global installs (a global dbt often lacks the `trino` adapter).
- Confirm `entropy-data connection test` succeeds. If not, stop and tell the user to run `entropy-data connection add default --api-key <key> --host http://<node-ip>:30808`.

Always use the `entropy-data` CLI for any connection to Entropy Data (data products, data contracts, access, publishing). Do not use the Entropy Data MCP server.

### Step 1: Resolve the data product

Accept either a full URL (`http://<node-ip>:30808/myorga/dataproducts/<id>`, extract the trailing id) or a bare id. Derive `DATA_PRODUCT_DIR` = `dags/dataproducts/<id>` and `DAG_ID` = `dataproduct_<id with - replaced by _>`.

- If `DATA_PRODUCT_DIR` does not exist, run **dataproduct-bootstrap** with this id (read its `SKILL.md`), then continue here.
- Run `entropy-data dataproducts get <id> -o yaml`. If it does not exist in Entropy Data yet, use the local ODPS file.
- If it exists, **merge** it with `<DATA_PRODUCT_DIR>/<id>.odps.yaml` instead of picking one side. Entropy Data is the source of truth for human-authored fields (`name`, `description`, `team`, `status`, `domain`, output port names and `contractId`s). The folder adds implementation details (`inputPorts`, output port `description`s, `tags`, the `dataProductBuilder` customProperty). Write the merged result to the local file and surface the diff.

Remember the result as `DATA_PRODUCT`. Extract:

- `DATA_PRODUCT_ID`, `DATA_PRODUCT_NAME`, owning team, purpose
- `inputPorts` (each has a `name`, usually the upstream data product id, and a `contractId`)
- `outputPorts` (each has a `name` and a `contractId`)

If the data product has more than one output port, ask the user which one(s) to implement. Default to all.

Make sure the top-level `customProperties` of the local ODPS contains `property: "dataProductBuilder"` with `value: "https://github.com/entropy-data/entropydata-dbt-demo-builder"`. Add it if missing.

### Step 2: Merge the data contract

For each selected output port, read `<DATA_PRODUCT_DIR>/<contract-id>.odcs.yaml` and run `entropy-data datacontracts get <contract-id> -o yaml`. Same merge rule as Step 1: Entropy Data wins for the human-authored fields (`schema` with its properties and descriptions, `description`, `team`, `status`); the folder only adds what Entropy Data lacks (e.g. the `trino` server). Write the merged result to the local file. Remember it as `CONTRACT`.

**Output port custom properties.** With the contract's schema and first table known, make sure every implemented output port in the local ODPS has these `customProperties` (add what is missing, keep other entries):

```yaml
customProperties:
  - property: displayName            # without it, Entropy Data shows "Deleted Output Port"
    value: <data product name>
  - property: openInLinks            # "Open In" menu entry for consumers with access
    value:
      - name: Superset (SQL Lab)
        url: ${SUPERSET_URL}/sqllab/?dbid=1&name=<url-encoded display name>&sql=<url-encoded SELECT * FROM "hive-iceberg".<schema>.<first table> LIMIT 100>
```

Keep `${SUPERSET_URL}` literally in the file: the Superset URL depends on the cluster, and the DAG (and Step 7) replace the placeholder when publishing. URL-encode name and SQL with `python3 -c 'import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))' '<text>'`; `dags/dataproducts/tpch-order-summary/tpch-order-summary.odps.yaml` is a working example.

After editing any ODCS file, run `datacontract lint <file>` yourself (the lint hook only fires for Edit/Write tool calls of an installed plugin; use the venv from Step 5 if `datacontract` is not on PATH yet) and fix lint errors.

You need from `CONTRACT`:

- `schema` (table name → properties list with `logicalType`, `physicalType`, `required`, `primaryKey`, `unique`, `description`, `classification`)
- `description.purpose` and the property descriptions: they describe the logic you implement in Step 4
- `servers`: expect one entry `server: trino`, `type: trino`, `host: trino-coordinator`, `port: 8443`, `catalog: hive-iceberg`, `schema: <schema>`. The schema must match `schema:` in `<DATA_PRODUCT_DIR>/profiles.yml`; if they disagree, ask which one to fix.

### Step 3: Input ports, sources and access

1. **Choose the input data products.** Start from `DATA_PRODUCT.inputPorts`. If it is empty or the contract needs data it does not cover, pick the upstream data products whose contracts provide the needed columns (default `tpch-core`; list `dags/dataproducts/` and read their ODCS files) and confirm the choice with the user. Add each to the ODPS `inputPorts` (`name` = upstream data product id, `version`, `contractId` = the upstream output port's `contractId`).

2. **Declare each input port as a dbt source** in `<DATA_PRODUCT_DIR>/models/_sources.yml`. Load the upstream contract (`dags/dataproducts/<input-port-name>/<contract-id>.odcs.yaml` if it is in the repo, else `entropy-data datacontracts get <contract-id> -o yaml`) and take `servers[0].catalog` / `servers[0].schema` (`tpch` / `tiny` for `tpch-source`, `hive-iceberg` / `demo` for `tpch-core` and the marts):

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
   ```

   Only list the tables you read. Do not copy upstream contracts into the folder: the DAG publishes every ODCS file in the folder and would overwrite the upstream owner's contract. `dags/dataproducts/tpch-order-summary/models/_sources.yml` (reads `tpch-core`) is a working example.

3. **Request access** to each input port as the consumer data product, so Entropy Data shows the data usage agreement. The provider output port id is the upstream ODPS `outputPorts[].id`, or its `name` if there is no `id` (e.g. `trino-core` for `tpch-core`); read it from `dags/dataproducts/<provider-id>/<provider-id>.odps.yaml` or `entropy-data dataproducts get <provider-id> -o yaml`. The consumer data product must exist in Entropy Data; if Step 1 found none, publish first (Step 7 commands). Skip agreements that already exist (`entropy-data access get <id>-<provider-id>`), then:

   ```
   entropy-data access request <provider-id> <provider-output-port-id> \
     --consumer-dataproduct <id> --id <id>-<provider-id> --purpose "<why this data product reads it>"
   entropy-data access approve <id>-<provider-id>
   ```

   If approval is refused (missing rights), leave the request pending and tell the user the provider team must approve it. The pipeline itself does not depend on the agreement.

### Step 4: Implement the dbt mart models

**Output column identifier rule.** For every contract property, resolve `OUT_COL = property.physicalName // property.name`. Use `OUT_COL` for the SQL alias **and** the `_marts.yml` `columns: - name:` entry so the projected column, the dbt tests, and the materialized Trino column all agree with the contract. Trino folds unquoted identifiers to lowercase; if `OUT_COL` is not all-lowercase, double-quote it in the SQL alias (`as "MixedCase"`) and flag it to the user.

For each table in each contract:

1. Model name = the table's `physicalName // name`. Write `<DATA_PRODUCT_DIR>/models/marts/<table>.sql`, starting with:

   ```sql
   -- Governed by <contract-id>.odcs.yaml (ODCS id: <CONTRACT_ID>)
   ```

2. **Implement the logic fully.** "Implement the data product" means a working, tested pipeline. Read the inputs with `{{ source('<source-name>', '<table>') }}` directly in the mart model, like the reference marts (`dags/dataproducts/tpch-order-summary/models/marts/order_summary.sql`). Do not add `stg_*` staging views: they would clash with `tpch-core`'s views in the same schema. Where the contract (`description.purpose`, property descriptions) or the ODPS describes joins, filters, aggregations or derived columns, write them in SQL so required columns and tests pass; join on the keys the upstream contracts document. Cast every output column to its Trino type: `cast(<expr> as <trino-type>) as <OUT_COL>`. Only when the logic is genuinely not derivable, use `cast(null as <trino-type>) as <OUT_COL> -- TODO: <what is missing>` and say so in the final report.

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

### Step 5: Python tooling and `dbt parse`

Create a venv inside the data product folder (gitignored via `.venv/`; add the line to `.gitignore` if missing) and put it first on PATH for every dbt, dbt-ol and datacontract call. `dbt-ol` calls whichever `dbt` is first on PATH, so a global dbt without the trino adapter breaks it.

```
cd <DATA_PRODUCT_DIR>
uv venv .venv && uv pip install --python .venv dbt-trino openlineage-dbt "datacontract-cli[trino]"
export PATH="$PWD/.venv/bin:$PATH"
TRINO_PASSWORD=${TRINO_PASSWORD:-unused} dbt parse --profiles-dir .
```

`dbt parse` does not connect to Trino, but the profile requires `TRINO_PASSWORD` to be set. If it fails, surface the error, fix obvious mistakes (wrong source name, typos in `_marts.yml`), and re-run. Do not proceed to Step 6 with a failing parse. Then delete the `target/` and `logs/` directories dbt created (they are gitignored anyway, but keep the folder clean).

### Step 6: Get it running

Ask the user: "Run it in Airflow (push to Forgejo, then trigger the DAG `<DAG_ID>`) or locally now?" Default to Airflow; it is the demo path and needs no local Trino access.

If any mart model still has TODOs, warn the user that the build or tests will fail those columns.

**Option A: Airflow (default).**

1. Show the commands. Run them only if the user explicitly asks you to:

   ```
   git add dags/dataproducts/<id>
   git commit -m "Implement data product <id>"
   git push
   ```

   The remote is `http://<node-ip>:30000/stackable/entropydata-dbt-demo.git`, branch `main` (user `stackable` / `stackable`).
2. Airflow git-syncs `dags/` within a minute or two. The user triggers `<DAG_ID>` in the Airflow UI (`admin` / `admin`). The DAG publishes the ODPS and all ODCS files, runs `dbt-ol build --profiles-dir .` with OpenLineage pointed at `api/v1/lineage?dataProductId=<id>`, and runs `datacontract test <file> --server trino --publish <entropy-data>/api/test-results` per output-port contract.
3. Mark the run steps `skipped (Airflow)` in the report; the user reads the results in Airflow and Entropy Data.

**Option B: Local.** Trino must be reachable: `kubectl port-forward svc/trino-coordinator 8443:8443`, then point the tools at it with env vars: `TRINO_HOST=localhost` (read by `profiles.yml`, along with `TRINO_PASSWORD` and `TRINO_CA_BUNDLE`) and `DATACONTRACT_TRINO_HOST=localhost` (overrides the contract's server host in the Data Contract CLI). If the TLS hostname check fails for `localhost`, add `127.0.0.1 trino-coordinator` to `/etc/hosts` instead and leave both unset. Require `TRINO_PASSWORD` in the environment and, if the Trino certificate is not in the system trust store, `TRINO_CA_BUNDLE`. Do not source credentials yourself.

Resolve the Entropy Data connection: use `ENTROPY_DATA_HOST` / `ENTROPY_DATA_API_KEY` if set (they override the saved connection), otherwise read the saved one. `-o` is a global option and goes before the subcommand; the key field is `api_key`:

```
ENTROPY_DATA_HOST=${ENTROPY_DATA_HOST:-$(entropy-data -o json connection get | jq -r .host)}
ENTROPY_DATA_API_KEY=${ENTROPY_DATA_API_KEY:-$(entropy-data -o json connection get --show-api-key | jq -r .api_key)}
```

Then run from `DATA_PRODUCT_DIR` with `.venv/bin` first on PATH, mirroring the DAG:

1. Build and ship lineage. Confirm first: "Run `dbt-ol build` against Trino now? This materializes the models in `hive-iceberg.<schema>`, runs the dbt tests, and ships the lineage event to Entropy Data." Wait for explicit yes.

   ```
   OPENLINEAGE__TRANSPORT__TYPE=http \
   OPENLINEAGE__TRANSPORT__URL=$ENTROPY_DATA_HOST \
   OPENLINEAGE__TRANSPORT__ENDPOINT="api/v1/lineage?dataProductId=<id>" \
   OPENLINEAGE__TRANSPORT__AUTH__TYPE=api_key \
   OPENLINEAGE__TRANSPORT__AUTH__APIKEY=$ENTROPY_DATA_API_KEY \
     dbt-ol build --profiles-dir .
   ```

   Non-zero exit means at least one model or test failed; surface the dbt log section, do not retry silently.

2. Data contract test, per output-port contract (`ENTROPY_DATA_HOST` must be set: the CLI only sends the API key to that host):

   ```
   DATACONTRACT_TRINO_USERNAME=${TRINO_USER:-admin} \
   DATACONTRACT_TRINO_PASSWORD=$TRINO_PASSWORD \
   REQUESTS_CA_BUNDLE=${TRINO_CA_BUNDLE:-} \
   ENTROPY_DATA_HOST=$ENTROPY_DATA_HOST ENTROPY_DATA_API_KEY=$ENTROPY_DATA_API_KEY \
     datacontract test <contract-id>.odcs.yaml --server trino --logs --publish $ENTROPY_DATA_HOST/api/test-results
   ```

3. Delete `target/` and `logs/` again after `dbt-ol build` (gitignored anyway).

Do not echo or persist API keys or passwords; export them only as part of the command that needs them.

### Step 7: Publish and activate

Publish the merged files, contracts first because the data product references them:

```
entropy-data datacontracts put <contract-id> --file <contract-id>.odcs.yaml   # per output-port contract
SUPERSET_URL=${SUPERSET_URL:-$(echo "${ENTROPY_DATA_HOST:-$(entropy-data -o json connection get | jq -r .host)}" | sed -E 's#:[0-9]+/?$#:30888#')}
sed "s#\${SUPERSET_URL}#${SUPERSET_URL}#g" <id>.odps.yaml > /tmp/<id>.odps.yaml
entropy-data dataproducts put <id> --file /tmp/<id>.odps.yaml
```

The `sed` fills the `${SUPERSET_URL}` placeholder of the Open In links (Superset runs on the same node as Entropy Data, NodePort 30888) without changing the file in the repo.

If the local `dbt-ol build` and every `datacontract test` passed, set `status: active` in the ODPS, publish it again, and say so in the report. On the Airflow path, tell the user to set `status: active` in the ODPS (or ask you to) once the DAG run is green, since the DAG re-publishes the folder's file. **The data product is now visible in Entropy Data with input ports, materialized tables, lineage, and test results.** Tell the user this explicitly; it is the whole point of the demo.

### Step 8: Final report

End with this two-part recap. Use the `Status` enum: `created`, `updated`, `already present`, `passed`, `failed`, `skipped`.

**Part 1: outcome table.**

| Artifact | Status | Details |
|---|---|---|
| Data product ODPS | … | `<DATA_PRODUCT_DIR>/<id>.odps.yaml` (merged with Entropy Data / local only) |
| `dataProductBuilder` customProperty | … | "added" / "already present" |
| Output-port data contract `<CONTRACT_ID>` | … | `<DATA_PRODUCT_DIR>/<contract-id>.odcs.yaml`, `datacontract lint` passed / failed |
| Input ports | … | `<N>` in the ODPS and `models/_sources.yml` |
| Access agreements | … | per input port: "approved" / "pending approval" / "already present" |
| Model `<table>.sql` | … | "implemented from `<sources>`" / "TODO: `<columns>`" / "skipped per user" |
| `_marts.yml` entries | … | counts |
| `dbt parse` | … | passed / failed: `<reason>` |
| `dbt-ol build` | … | "skipped (Airflow)" / "passed: N models, M tests, lineage shipped" / "failed" |
| `datacontract test` | … | "skipped (Airflow)" / per contract: "passed" / "failed: <count>" |
| Publish ODPS + ODCS | … | "passed" / "failed" |
| Status | … | "active" / "unchanged: <reason>" |

**Part 2: next steps.** Bullet list, only what applies:

- For each remaining TODO column, why the logic is not derivable from the contract and what the user needs to specify.
- For each `failed` row, the concrete next action (which model, which test, which contract rule).
- For each pending access request, who has to approve it.
- For the Airflow path: the push commands (if not run yet) and "trigger `<DAG_ID>` in the Airflow UI".
- Link the user to `http://<node-ip>:30808/myorga/dataproducts/<id>` to see the data product, lineage, and test results land.

If everything passed and there are no TODOs, write: `Pipeline implemented, materialized, tested, lineage published, and data product set to active. Nothing else to do.`

## Constraints

- **Trino on Stackable only.** The type map, profile, and CLI invocations target the demo's Trino. If `profiles.yml` is not `type: trino`, stop.
- **Contract describes schema and logic.** Columns, types, and tests come from the contract; joins, aggregations, and derivations come from its purpose and property descriptions. Do not invent business logic the contract does not describe; leave a TODO and report it instead.
- **No staging layer.** Marts read `source(...)` directly; no `stg_*` models.
- **No secrets in the folder.** Airflow git-syncs it; credentials come from env vars only.
- **Don't overwrite existing dbt SQL silently.** Surface the diff and ask.
- **Don't auto-fix failing dbt or datacontract tests by weakening the contract.** Fix your own SQL; report contract or data issues to the user.
- **Don't commit or push** unless the user explicitly asks.
- **Idempotent.** Re-running on the same data product when files already match the contract is a no-op (except the run/test commands, which always re-execute).
