# {{DATA_PRODUCT_NAME}}

Data product `{{DATA_PRODUCT_ID}}` on the Stackable demo: a dbt project on Trino, writing to `"hive-iceberg".{{SCHEMA}}`. Published to [Entropy Data](https://entropy-data.com) by the Airflow DAG `{{DAG_ID}}`.

## Files

```
{{DATA_PRODUCT_ID}}.odps.yaml    # data product (ODPS)
{{CONTRACT_FILE}}                # output-port data contract (ODCS)
dbt_project.yml
profiles.yml                     # Trino connection, credentials come from env vars
models/
├── _sources.yml                 # input ports as dbt sources, read directly by the marts
└── marts/                       # output-port tables, one per table in the contract
```

## Run in Airflow

Commit and push to the in-cluster Forgejo repo (`http://<node-ip>:30000/stackable/entropydata-dbt-demo`, branch `main`). Airflow git-syncs `dags/` and picks up this folder as the DAG `{{DAG_ID}}`. Trigger it in the Airflow UI. The DAG:

1. publishes `{{DATA_PRODUCT_ID}}.odps.yaml` and `{{CONTRACT_FILE}}` to Entropy Data,
2. runs `dbt-ol build --profiles-dir .` and ships OpenLineage to Entropy Data,
3. runs `datacontract test {{CONTRACT_FILE}} --server trino --publish <entropy-data>/api/test-results`.

## Run locally

Only needed for debugging; the steps mirror the DAG. Forward Trino with `kubectl port-forward svc/trino-coordinator 8443:8443` and point the tools at it with `TRINO_HOST` (dbt) and `DATACONTRACT_TRINO_HOST` (Data Contract CLI). If the TLS hostname check fails for `localhost`, add `127.0.0.1 trino-coordinator` to `/etc/hosts` and leave both unset.

```bash
uv venv .venv
uv pip install --python .venv dbt-trino openlineage-dbt 'datacontract-cli[trino]'
export PATH="$PWD/.venv/bin:$PATH"   # dbt-ol calls the first dbt on PATH

export TRINO_HOST=localhost DATACONTRACT_TRINO_HOST=localhost
export TRINO_PASSWORD=<trino-admin-password>
export TRINO_CA_BUNDLE=<path-to-trino-ca.crt>   # or leave unset to use the system trust store
export ENTROPY_DATA_HOST=http://<node-ip>:30808
export ENTROPY_DATA_API_KEY=<api-key>

OPENLINEAGE__TRANSPORT__TYPE=http \
OPENLINEAGE__TRANSPORT__URL=$ENTROPY_DATA_HOST \
OPENLINEAGE__TRANSPORT__ENDPOINT="api/v1/lineage?dataProductId={{DATA_PRODUCT_ID}}" \
OPENLINEAGE__TRANSPORT__AUTH__TYPE=api_key \
OPENLINEAGE__TRANSPORT__AUTH__APIKEY=$ENTROPY_DATA_API_KEY \
  dbt-ol build --profiles-dir .

DATACONTRACT_TRINO_USERNAME=admin DATACONTRACT_TRINO_PASSWORD=$TRINO_PASSWORD \
REQUESTS_CA_BUNDLE=${TRINO_CA_BUNDLE:-} \
  datacontract test {{CONTRACT_FILE}} --server trino --logs --publish $ENTROPY_DATA_HOST/api/test-results
```

`.venv/`, `target/` and `logs/` are gitignored.

No secrets in this folder: `profiles.yml` reads every credential from env vars.
