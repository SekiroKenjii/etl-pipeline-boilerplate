# Getting started

Requirements: Python 3.14 and [uv](https://docs.astral.sh/uv/). For the
databases of the integration tests, podman or Docker.

```sh
make install       # .venv with every extra, the dev tools, the git hooks
make demo          # SQLite demo databases, a CRM load, a cutover report
make check         # lint, types, tests (SQLite), compiled mappings
```

`make check` is the quick gate. CI also runs the tests on every database,
with the coverage floor (`make cov-db`), `make security` and `make docs`.

With the test databases (PostgreSQL, SQL Server, Oracle, MySQL, MongoDB,
Kafka, Seq, Vault, an OpenTelemetry Collector):

```sh
make db-up && make test-db && make db-down
```

The demo loads the CRM module's legacy data. Its rows are dirty on purpose:
a date that does not exist, a country no lookup knows, and an order whose
customer is missing.

```{program-output} etl --env dev list
:cwd: ..
```

On fresh demo databases (`make demo` runs most of these):

```sh
uv run etl --env dev run crm.full_load      # exit 3, partial success: 3 rejected
uv run etl --env dev quarantine list        # 2 customers, and why
uv run etl --env dev parked list order      # the order waiting for its customer
uv run etl --env dev cutover report crm     # exit 1, FAILED: rejects not accepted
```

The two rejected customers will never be loaded. Once the BAs accept to
leave them behind, the report passes with them:

```sh
printf 'entity,reason,rows\ncustomer,*,2\n' > accepted.csv
uv run etl --env dev cutover report crm --accept-rejections accepted.csv
# exit 3, PASSED-WITH-REJECTS: the missing order is the orphan parked
```

## A module of your own

```sh
uv run etl new-module billing
```

It writes two files. `src/modules/billing/__init__.py`, about fifty lines,
is the module: a `core.mapping_pipelines.MappingModule` with its settings and its
custom transforms. `mappings/billing.xlsx` is the workbook the BAs fill,
started from `etl-mappings template`, with two example mappings to
replace.

The module comes with the pipelines of the CRM module, built from the
workbook: `full_load` (by watermark, or by key ranges in parallel), `sync`
(change data capture), `parked`, `replay` and `deletes`, each mapping a
step in the order its references need, its rows checked by the quality
gate, its surrogate keys registered, its steps retried, and its
reconciliations ready for `etl cutover report billing`. Writing it takes
four names:

| Name | What it is |
| --- | --- |
| `config.MappingModuleSettings` | The settings of the module, `[modules.billing.settings]`: databases, compiled workbook, watermark, orphans, partitions... The module subclasses it with its defaults. |
| `mapping.TransformRegistry` | The module's custom transforms, which the workbook can then pick. |
| `mapping_pipelines.MappingModule` | The module: its name, settings and transforms give its pipelines. |
| `create` | The function the settings point to: `module = "modules.billing:create"`. |

Then, as `etl new-module` prints:

```sh
uv run etl-mappings --registry modules.billing:registry compile \
    mappings/billing.xlsx
# config/base.toml: [modules.billing], with its source and target databases
uv run etl run billing.full_load
```

A pipeline the five do not cover is a subclass of
`mapping_pipelines.MappingPipeline` whose `job` builds the step of a mapping from
the shared parts (`etl_job`, `transformer`, `loader`, `gate`), returned by
the module's `pipelines` next to the others.

`MappingModuleSettings` refuses a pool too small for the key ranges read
at once (`partitions`). A module with settings of its own that runs steps
in parallel checks its pools itself, with `AppSettings.check_pools`.
