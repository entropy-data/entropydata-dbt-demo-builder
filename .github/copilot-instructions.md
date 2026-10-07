# Copilot instructions: entropydata-dbt-demo-builder

This repository is a coding-agent plugin that helps build **Trino** dbt data products in the Stackable + Entropy Data demo (repo `entropydata-dbt-demo`). It is a demo derivative of [dataproduct-builder-demo](https://github.com/entropy-data/dataproduct-builder-demo).

Capabilities are exposed as **skills**: markdown files under `skills/<name>/SKILL.md` that the agent reads top-to-bottom and executes step by step.

## Routing

When a user request matches a skill's trigger, **read the corresponding `SKILL.md` start to finish before acting.**

| When the user asks about… | Follow this skill |
|---|---|
| Scaffolding a brand-new data product folder `dags/dataproducts/<id>/` in the demo repo | `skills/dataproduct-bootstrap/SKILL.md` |
| Implementing a data product from an Entropy Data URL or id, then running it via the Airflow DAG or locally (publish, `dbt-ol build`, data contract tests) | `skills/dataproduct-implement/SKILL.md` |
| Testing existing data contracts against the live Trino server | `skills/datacontract-test/SKILL.md` |

The trigger phrasing above is illustrative; each `SKILL.md`'s frontmatter `description` is authoritative.

## Resolving `${PLUGIN_ROOT}`

Skill files reference `${PLUGIN_ROOT}` to locate `templates/`. On Claude Code it is set automatically as `${CLAUDE_PLUGIN_ROOT}`. On Copilot / Codex / Cursor, it is **not** set; resolve it as the directory containing `AGENTS.md` and `skills/`.

## CLIs the skills shell out to

- **`entropy-data`**: fetch and publish data products / contracts, list teams.
- **`dbt`** (`dbt-trino` adapter): `dbt parse` and local runs.
- **`dbt-ol`** (`openlineage-dbt`): runs dbt with OpenLineage so lineage ships to Entropy Data.
- **`datacontract`** (`datacontract-cli[trino]`): runs contract tests against Trino.

If any CLI is missing, surface the install line and stop.

## Conventions

- **Trino on Stackable only.** If the data product's `profiles.yml` is not Trino, stop and point the user at https://github.com/entropy-data/dataproduct-builder-dbt for the multi-platform builder.
- **One folder per data product.** Everything lives in `dags/dataproducts/<id>/`; the generic Airflow DAG `dataproduct_<id with - replaced by _>` publishes, builds, and tests it after a push to Forgejo.
- **Don't skip pre-checks.** The skills verify the demo repo checkout, CLI presence, and the Entropy Data connection first.
- **Don't overwrite existing files silently.** Surface the diff and ask.
- **Don't commit or push** unless the user explicitly asks.
- **Don't commit secrets.** Trino passwords and API keys must come from env vars.
- **Idempotent re-runs**, except `dbt-ol build` / `datacontract test` which always execute.
