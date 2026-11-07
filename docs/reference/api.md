# Python API

The ports a module and an adapter work with, and the operators built on them.

## Contracts

```{eval-rst}
.. automodule:: core.contract
   :no-members:
.. autoclass:: core.contract.ITableReader
.. autoclass:: core.contract.ITableWriter
.. autoclass:: core.contract.IChangeSource
.. autoclass:: core.contract.IMessageSource
.. autoclass:: core.contract.IQuarantine
.. autoclass:: core.contract.IKeyRegistry
.. autoclass:: core.contract.IRunLedger
.. autoclass:: core.contract.PipelineStep
.. autoclass:: core.contract.Reconciliation
```

## Runtime

```{eval-rst}
.. autoclass:: core.runtime.Module
.. autoclass:: core.runtime.Resources
   :members: get, run_scope, health, close
.. autoclass:: core.runtime.Kernel
   :members: run, plan, check
```

## Mapping-driven modules

```{eval-rst}
.. automodule:: core.mapping_pipelines
   :no-members:
.. autoclass:: core.mapping_pipelines.MappingModule
   :members: mappings, load, lookups
.. autoclass:: core.mapping_pipelines.MappingPipeline
   :members: job, jobs, prepare, etl_job, transformer, loader, gate
.. autoclass:: core.mapping_pipelines.Mappings
   :members: source_table, target_table, natural_key, lineage
.. autoclass:: core.mapping_pipelines.MappingSettings
.. autoclass:: config.MappingModuleSettings
```

## Operators

```{eval-rst}
.. automodule:: core.operators
   :no-members:
.. autoclass:: core.operators.TableExtractor
   :members: query
.. autoclass:: core.operators.TableLoader
.. autoclass:: core.operators.ChangeExtractor
.. autoclass:: core.operators.KeyPartitions
   :members: ranges, extractors
.. autoclass:: core.operators.QuarantineReplay
.. autoclass:: core.operators.Corrections
```

## Cutover

```{eval-rst}
.. automodule:: core.cutover
   :no-members:
.. autofunction:: core.cutover.reconcile
.. autofunction:: core.cutover.build
.. autoclass:: core.cutover.CutoverReport
   :members: verdict, as_dict
.. autoclass:: core.cutover.AcceptedRejections
   :members: from_csv
.. autofunction:: core.cutover.seal
.. autofunction:: core.cutover.verify
.. autofunction:: core.cutover.compare
.. autoclass:: core.cutover.LoadedRowsCheck
```
