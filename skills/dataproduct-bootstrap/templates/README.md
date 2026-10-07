# {{DATA_PRODUCT_NAME}}

Data product `{{DATA_PRODUCT_ID}}` on the Stackable demo: a dbt project on Trino, writing to `"hive-iceberg".{{SCHEMA}}`. Published to [Entropy Data](https://entropy-data.com) by the Airflow DAG `{{DAG_ID}}`.

## Files

```
{{DATA_PRODUCT_ID}}.odps.yaml    # data product (ODPS)
{{CONTRACT_FILE}}                # output-port data contract (ODCS)
dbt_project.yml
profiles.yml                     # Trino connection, credentials come from env vars
models/
├── staging/                     # input-port sources + 1:1 cleaned views
└── marts/                       # output-port tables, one per table in the contract
```

## Run in Airflow

Commit and push to the in-cluster Forgejo repo (`http://<node-ip>:30000/stackable/entropydata-dbt-demo`, branch `main`). Airflow git-syncs `dags/` and picks up this folder as the DAG `{{DAG_ID}}`. Trigger it in the Airflow UI. The DAG:

1. publishes `{{DATA_PRODUCT_ID}}.odps.yaml` and `{{CONTRACT_FILE}}` to Entropy Data,
2. runs `dbt-ol build --profiles-dir .` and ships OpenLineage to Entropy Data,
3. runs `datacontract test {{CONTRACT_FILE}} --server trino --publish <entropy-data>/api/test-results`.

## Run locally

Only needed for debugging. Trino must be reachable as `trino-coordinator:8443`, e.g. via `kubectl port-forward svc/trino-coordinator 8443:8443` plus a `127.0.0.1 trino-coordinator` entry in `/etc/hosts`.

```bash
uv venv
source .venv/bin/activate
uv pip install dbt-core dbt-trino openlineage-dbt 'datacontract-cli[trino]' entropy-data

export TRINO_PASSWORD=<trino-admin-password>
export TRINO_CA_BUNDLE=<path-to-trino-ca.crt>   # or leave unset to use the system trust store

dbt build --profiles-dir .
DATACONTRACT_TRINO_USERNAME=admin DATACONTRACT_TRINO_PASSWORD=$TRINO_PASSWORD \
  datacontract test {{CONTRACT_FILE}} --server trino --logs
```

No secrets in this folder: `profiles.yml` reads every credential from env vars.
