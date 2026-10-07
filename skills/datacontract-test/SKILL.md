---
name: datacontract-test
description: Run `datacontract test` against the ODCS contracts of a data product in the Stackable demo to verify the live Trino data still conforms to schema, quality rules, and freshness. Handles two kinds of contracts with different semantics: output-port contracts in `dags/dataproducts/<id>/*.odcs.yaml` (tested against this data product's `hive-iceberg` schema: "am I still producing what I promised?") and input-port contracts referenced by the ODPS `inputPorts` (tested against the upstream server: "is upstream still producing what I trusted?"). Trigger when the user asks to "test the data contracts", "verify the data product matches its contract", "are we still contract-conformant", "check upstream drift", or "run the contract tests".
---

# Test ODCS data contracts against Trino

Run the **Data Contract CLI** (`datacontract test`) against contracts of a data product to check whether the data currently in Trino still matches the schema and quality rules declared in the contract. The Airflow DAG runs the same test after every build; this skill reproduces it on demand.

Two kinds of contracts matter here and they test against different Trino schemas:

- **Output-port contracts** at `dags/dataproducts/<id>/<contract-id>.odcs.yaml`, referenced by the ODPS `outputPorts[].contractId`. What this data product commits to produce, usually in `"hive-iceberg".<schema>`. A failure means we are no longer producing what we promised.
- **Input-port contracts**, referenced by the ODPS `inputPorts[].contractId`. They live in the upstream data product's folder (e.g. `dags/dataproducts/tpch-source/tpch-source.odcs.yaml`) or in Entropy Data. They test against the **upstream** server block (e.g. `tpch.tiny`). A failure means upstream drifted from the contract we trusted; the consequence is that our output may break too. Treat input-port failures as an upstream incident, not a local bug.

## When to use this vs. other skills

- **Just ran `dataproduct-implement` and want to re-test without re-running dbt** → this skill.
- **The Airflow DAG failed the contract test task** → this skill, to reproduce locally with `--logs`.
- **You want to edit a contract and check the impact** → not in this demo plugin; use [datacontract-edit](https://github.com/entropy-data/dataproduct-builder-dbt/blob/main/skills/datacontract-edit/SKILL.md) from the full builder.

## How to run this skill

> `${PLUGIN_ROOT}` below refers to the root of this plugin, the directory that contains `skills/`. On Claude Code it is set automatically as `${CLAUDE_PLUGIN_ROOT}`; use that. On any other agent (Codex, Copilot CLI, etc.) it is unset; resolve it as `../..` relative to **this `SKILL.md` file's directory** (i.e. the grandparent of `skills/<this-skill>/`).

### Plan announcement (before Step 0)

Before running Step 0, print this plan to the user verbatim:

> Running **datacontract-test**. I'll:
> 1. Pre-checks: confirm `datacontract` is on PATH, Trino credentials are in the environment, and `trino-coordinator:8443` is reachable.
> 2. Pick which contract(s) to test: defaults to the data product's output-port and input-port contracts.
> 3. Pick the server (defaults to `trino`).
> 4. Run `datacontract test` per contract and capture the result.
> 5. Report pass/fail with per-rule detail; flag missing credentials separately from real failures.

Then proceed.

### Step 0: Pre-checks

- Confirm `datacontract --version` is on PATH. Prefer the data product's venv if it exists (`export PATH="$PWD/dags/dataproducts/<id>/.venv/bin:$PATH"`, created by dataproduct-implement). If neither is available, stop and tell the user to install it: `uv tool install 'datacontract-cli[trino]'`.
- Confirm at least one `*.odcs.yaml` exists under `dags/dataproducts/*/`. If not, stop and tell the user there is nothing to test.
- Confirm Trino credentials for the Data Contract CLI are in the environment. The CLI reads `DATACONTRACT_TRINO_*` env vars, **not** `profiles.yml`:

  ```bash
  DATACONTRACT_TRINO_USERNAME    # admin
  DATACONTRACT_TRINO_PASSWORD    # same as TRINO_PASSWORD for dbt
  ```

  If any are unset, surface the list and ask whether to continue (the CLI will fail fast on that server) or stop. Do not try to source credentials yourself.
- Confirm Trino is reachable on port 8443. Outside the cluster that needs `kubectl port-forward svc/trino-coordinator 8443:8443` plus `DATACONTRACT_TRINO_HOST=localhost`, which overrides the contract's server host (if the TLS hostname check fails for `localhost`, add `127.0.0.1 trino-coordinator` to `/etc/hosts` instead). If the Trino certificate is not in the system trust store, set `REQUESTS_CA_BUNDLE` to the CA file. If not reachable, say so and suggest re-running the Airflow DAG instead, which tests from inside the cluster.

### Step 1: Select contracts

- If the user named a data product id, read `dags/dataproducts/<id>/<id>.odps.yaml`. Output-port contracts are the files `<contractId>.odcs.yaml` for each `outputPorts[].contractId`. Input-port contracts are `inputPorts[].contractId`: use `dags/dataproducts/<input-port-name>/<contractId>.odcs.yaml` if present, else fetch with `entropy-data datacontracts get <contractId> -o yaml` into a temporary directory outside the repo (never into the data product folder: the DAG would publish it).
- If the user named a specific contract file, use it.
- If the user said "output contracts" / "input contracts" / "upstream drift", scope to one of those.
- If they named nothing, list the data product folders and ask which one. Then list its contracts grouped by **Output ports** and **Input ports** so the user sees the two roles, and ask before running.
- Remember the resolved list as `CONTRACTS`. For each entry, also remember its role (`output` or `input`); Step 4 surfaces failures differently.

### Step 2: Select the server

For each contract in `CONTRACTS`:

- If the contract has exactly one server, use it. In the demo that is always `trino`.
- If it has multiple, default to `trino`. If `trino` is not defined, ask the user which one.
- Only pass `--server all` if the user explicitly asks to test every server.

### Step 3: Run the test

For each contract:

```
datacontract test <path-to-contract>.odcs.yaml --server trino --logs
```

- `--logs` ensures per-rule failure detail is in stdout; without it the CLI only prints a summary.
- If the user asks for a persisted report, add `--output ./test-results/<contract>.xml --output-format junit` (outside the data product folder).
- If the user asks to publish results back to Entropy Data (what the Airflow DAG does), add `--publish $ENTROPY_DATA_HOST/api/test-results` with `ENTROPY_DATA_HOST` and `ENTROPY_DATA_API_KEY` set inline (the CLI only sends the key to that host). Use the env vars if already set, otherwise read the saved connection: `entropy-data -o json connection get | jq -r .host` and `entropy-data -o json connection get --show-api-key | jq -r .api_key`. Do not publish by default; it writes server-side state.
- Capture stdout and exit code per contract. Non-zero exit means at least one rule failed.

Run sequentially, not in parallel: Trino is the bottleneck and parallel runs muddy the log output.

### Step 4: Report

End with this two-part recap. Use the `Status` enum: `passed`, `failed`, `skipped` (missing creds or Trino unreachable).

**Part 1: outcome table.** One row per contract tested. Group the rows: output-port contracts first, then input-port contracts under a sub-header (so the reader sees the two roles at a glance).

| Contract | Role | Server | Result | Failures | Details |
|---|---|---|---|---|---|
| `<contract-file>` | `output` / `input` | `trino` | `passed` / `failed` / `skipped` | count or `n/a` | one line per failing rule (field + rule), or "missing env var: …" if skipped |

**Part 2: next steps.** Bullet list, include only what applies. Treat output vs. input failures differently:

- **Output-port failures**: surface the field and the violated check (e.g. `order_summary.order_id: not_null violated for 17 rows`). The fix is in this data product: either the dbt model is wrong, the contract is wrong, or the data is wrong. If the user wants a follow-up SQL to find the offending rows, suggest the shape but do not run it.
- **Input-port failures**: this is upstream drift. Name the provider data product and output port. The fix is *not* in this data product; the user should contact the upstream owner, and in the meantime expect downstream output-port failures.
- For each `skipped` row, the exact env vars or port-forward the user needs.
- If failures look like a data quality issue (rules unchanged, data drifted), suggest investigating the upstream of the failing model; this skill does not auto-fix data.

If everything passed, write a single line: `All <N> contracts pass against trino.`

## Trino authentication

The Data Contract CLI reads credentials from environment variables, not from the contract file. Only the connection topology belongs in the `servers` block:

```yaml
servers:
  - server: trino
    type: trino
    host: trino-coordinator
    port: 8443
    catalog: hive-iceberg
    schema: demo
```

The demo's Trino uses basic auth over HTTPS (user `admin`). Setting a password switches the CLI to HTTPS:

```bash
export DATACONTRACT_TRINO_USERNAME=admin
export DATACONTRACT_TRINO_PASSWORD=...
```

## Constraints

- **Read-only against Trino.** This skill runs `datacontract test` which executes `SELECT` queries; it never writes. Do not invoke `datacontract publish`, `datacontract export`, or `entropy-data datacontracts put` from this skill.
- **No edits to contracts or models.** If a test fails, surface it; do not auto-patch the contract to make it pass. That defeats the purpose.
- **No credential sourcing.** If env vars are missing, tell the user; don't read them from `.env`, dotfiles, Kubernetes secrets, or anywhere else on the user's behalf.
- **Idempotent**: re-running the skill produces the same report against the same data. Failures from rules that depend on time (freshness, row-count windows) are expected to drift; note that in the failure detail when relevant.
