# entropydata-dbt-demo-builder

Demo-grade coding-agent plugin for building **Trino** dbt data products in the Stackable + Entropy Data demo (repo `entropydata-dbt-demo`: Trino, Airflow, dbt-ol, Entropy Data Community Edition).

This is the Stackable sibling of [dataproduct-builder-demo](https://github.com/entropy-data/dataproduct-builder-demo) (Snowflake + GitHub Actions). Same three skills and one batched question, but a data product is a folder in the demo repo and a generic Airflow DAG does the publishing, building, lineage, and testing. No CI workflow, no OpenLineage config, no secrets in the repo.

## How a data product runs

```
dags/dataproducts/<id>/
├── <id>.odps.yaml             # data product (ODPS)
├── <contract-id>.odcs.yaml    # one data contract (ODCS) per output port
├── dbt_project.yml            # optional dbt project on Trino
├── profiles.yml               # credentials from env vars
└── models/
```

Push the folder to the in-cluster Forgejo repo (`http://<node-ip>:30000/stackable/entropydata-dbt-demo`, branch `main`). Airflow git-syncs `dags/` and creates the DAG `dataproduct_<id with - replaced by _>`, which:

1. publishes the ODPS and all ODCS files to Entropy Data,
2. runs `dbt-ol build` against Trino and ships OpenLineage to Entropy Data,
3. runs `datacontract test --server trino --publish` for each output-port contract.

The demo ships two reference data products: `tpch-source` (raw TPC-H tables in `tpch.tiny`, ODPS + ODCS only) and `tpch-analytics` (dbt staging views and mart tables in `"hive-iceberg".demo`). New data products typically read from one of their output ports.

## Skills

- **[dataproduct-bootstrap](skills/dataproduct-bootstrap/SKILL.md)** scaffolds a new data product folder: ODPS, output-port ODCS, `dbt_project.yml`, `profiles.yml` for Trino, staging + marts model layout, README.
- **[dataproduct-implement](skills/dataproduct-implement/SKILL.md)** syncs a data product and its contract from Entropy Data, generates dbt models from the ODCS contract, runs `dbt parse`, and then hands the folder to the Airflow DAG (push to Forgejo, trigger the DAG) or runs the same steps locally.
- **[datacontract-test](skills/datacontract-test/SKILL.md)** runs `datacontract test` against output-port and input-port ODCS files to verify the live Trino data still matches the contract.

## Install

The skills are plain markdown; any coding agent that can read instruction files can run them.

### Claude Code

```
/plugin marketplace add https://github.com/entropy-data/entropydata-dbt-demo-builder
/plugin install entropydata-dbt-demo-builder@entropydata-dbt-demo-builder
```

### OpenAI Codex

```
codex plugin marketplace add https://github.com/entropy-data/entropydata-dbt-demo-builder
```

Then open Codex, run `/plugins`, and pick `entropydata-dbt-demo-builder`.

### GitHub Copilot CLI

```
/plugin marketplace add https://github.com/entropy-data/entropydata-dbt-demo-builder
/plugin install entropydata-dbt-demo-builder@entropydata-dbt-demo-builder
```

### Other agents (Cursor, Aider, etc.)

Any agent that reads `AGENTS.md` picks up the routing manifest. Or copy the `skills/` directory into the path your coding agent expects.

## Connect

The skills talk to Entropy Data through a connection registered with the [entropy-data CLI](https://github.com/entropy-data/entropy-data-cli) (requires [uv](https://docs.astral.sh/uv/)).

Open Entropy Data at `http://<node-ip>:30808`, log in as `demo-admin`, create an API key (**Organization Settings → API Keys → Create new API key**), and add the connection:

```
uv tool install --upgrade entropy-data
entropy-data connection add default --api-key <your-api-key> --host http://<node-ip>:30808
```

Clone the demo repo from Forgejo (user `stackable` / `stackable`) and run the agent in its root:

```
git clone http://<node-ip>:30000/stackable/entropydata-dbt-demo.git
cd entropydata-dbt-demo
```

## Use

Ask the agent:

> Bootstrap a new data product that reads from tpch-analytics.

Then, once the folder is in place:

> Implement the data product *url or id*.

The implement skill generates the models from the contract, checks them with `dbt parse`, and gets the data product running: commit and push to Forgejo, then trigger the DAG in Airflow. The data product, its lineage, and its test results show up in Entropy Data. Running locally works too, with a `kubectl port-forward` to Trino.

## Trino-only

This demo plugin targets the demo's Trino (`trino-coordinator:8443`, catalogs `tpch` and `hive-iceberg`). The templates are Trino-shaped and the runtime invocations use the `dbt-trino` adapter and the `datacontract-cli[trino]` extras.

For Snowflake with GitHub Actions, use [dataproduct-builder-demo](https://github.com/entropy-data/dataproduct-builder-demo). For Databricks / BigQuery / Postgres, use the full builder at https://github.com/entropy-data/dataproduct-builder-dbt. The plugins can coexist.

## License

MIT
