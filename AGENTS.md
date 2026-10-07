# entropydata-dbt-demo-builder: agent manifest

> **This file is the plugin's authoritative routing manifest, not a template.** It lives at the plugin's repo root and is meant to be **referenced** from your project (e.g. via Codex CLI's marketplace install, or a one-line pointer in your project's own `AGENTS.md`), not copied into your project. Updating the plugin updates this file in place.

This repository is a coding-agent plugin that helps build **Trino** dbt data products in the Stackable + Entropy Data demo (repo `entropydata-dbt-demo`: Trino, Airflow, dbt-ol, Entropy Data Community Edition). It is a demo derivative of [dataproduct-builder-demo](https://github.com/entropy-data/dataproduct-builder-demo). It exposes its capabilities as **skills**: markdown files under `skills/<name>/SKILL.md` that you read top-to-bottom and execute step by step.

When a user request matches a skill's trigger, **read the corresponding `SKILL.md` start to finish before acting.** Each skill contains pre-checks, parameter-gathering, and explicit user-confirmation gates that must not be skipped.

## Skills

| When the user asks about… | Follow this skill |
|---|---|
| Scaffolding a brand-new data product folder `dags/dataproducts/<id>/` in the demo repo | `skills/dataproduct-bootstrap/SKILL.md` |
| Implementing a data product from an Entropy Data URL or id: merge ODPS/ODCS, add input ports and access requests, implement dbt models from the ODCS contract, then run it via the Airflow DAG or locally (`dbt-ol build`, data contract tests, publish) | `skills/dataproduct-implement/SKILL.md` |
| Testing existing data contracts against the live Trino server (no edits, just run `datacontract test`) | `skills/datacontract-test/SKILL.md` |

The trigger phrasing above is illustrative; each `SKILL.md`'s frontmatter `description` is authoritative.

## Resolving `${PLUGIN_ROOT}`

The skill files reference `${PLUGIN_ROOT}` to locate `templates/`. On Claude Code this is set automatically as `${CLAUDE_PLUGIN_ROOT}`; on Codex / Cursor / other agents reading this file, it is **not** set. Resolve it as **the directory that contains this `AGENTS.md`** (the cloned repo root, which also contains `skills/`).

## How the demo runs a data product

- A data product is one folder in the demo repo: `dags/dataproducts/<id>/` with `<id>.odps.yaml` (ODPS v1.0.0), one `<contract-id>.odcs.yaml` (ODCS v3.1.0) per output port, and optionally a dbt project (`dbt_project.yml`, `profiles.yml`, `models/`). No secrets in the folder.
- Users push to the in-cluster Forgejo repo (`http://<node-ip>:30000/stackable/entropydata-dbt-demo`, branch `main`). Airflow git-syncs `dags/`.
- The generic DAG factory `dags/dataproduct_dags.py` creates one DAG per folder, `dataproduct_<id with - replaced by _>`, which publishes the ODPS and ODCS files to Entropy Data, runs `dbt-ol build --profiles-dir .` with OpenLineage to Entropy Data, and runs `datacontract test --server trino --publish` per output-port contract.
- Trino: `trino-coordinator:8443`, HTTPS, basic auth (user `admin`, password from env). Catalogs `tpch` (schema `tiny`, raw source data) and `hive-iceberg` (dbt output schemas).
- Entropy Data CE: `http://entropy-data:8080` in-cluster, `http://<node-ip>:30808` from outside, organization `myorga`, Keycloak users `demo-admin` / `demo-user`.
- Reference data products: `tpch-source` (source-aligned, ODPS + ODCS only), `tpch-core` (dbt staging views in `"hive-iceberg".demo`) and six marts such as `tpch-order-summary` (one dbt mart table reading `tpch-core` via `source(...)` directly, no staging layer of its own; one output port and data contract each, input port from `tpch-core`).

## CLIs the skills shell out to

- **`entropy-data`** (PyPI: `entropy-data`; install with `uv tool install entropy-data`): used to fetch data products and contracts, publish updates, request and approve access, list teams. Connect with `entropy-data connection add default --api-key <key> --host http://<node-ip>:30808`. `ENTROPY_DATA_HOST` / `ENTROPY_DATA_API_KEY` override the saved connection.
- **`dbt`** (`dbt-trino` adapter): used by the implement skill for `dbt parse` and local runs. The implement skill installs dbt, dbt-ol and datacontract into a `.venv` inside the data product folder (`uv venv .venv && uv pip install --python .venv dbt-trino openlineage-dbt "datacontract-cli[trino]"`) and puts `.venv/bin` first on PATH.
- **`dbt-ol`** (PyPI: `openlineage-dbt`): runs dbt with OpenLineage so lineage ships to Entropy Data. Used by the Airflow DAG and by local runs.
- **`datacontract`** (PyPI: `datacontract-cli[trino]`): runs contract tests against the Trino server defined in the ODCS file.

If any CLI is missing, surface the install line and stop. Do not try to install on the user's behalf without confirmation (the per-folder `.venv` the implement skill creates is the exception).

## Conventions when running skills

- **Trino on Stackable only.** This demo plugin targets the demo's Trino. For Snowflake, Databricks, BigQuery or Postgres, point users at https://github.com/entropy-data/dataproduct-builder-dbt.
- **Don't skip the pre-checks.** The skills verify the demo repo checkout, CLI presence, and the Entropy Data connection before touching files.
- **Don't overwrite existing files silently.** Surface the diff and ask.
- **Don't commit or push** on the user's behalf unless they explicitly ask. Surface the commands instead.
- **Don't commit secrets.** The data product folder is git-synced into Airflow; Trino passwords and API keys come from env vars only.
- **Idempotent re-runs.** Running a skill a second time when everything is already in place should be a no-op, except for `dbt-ol build` / `datacontract test` which always re-execute by design.
