# Types

Where the types of the project live, and all of them on one page. A type
has one of three homes, which the architecture tests enforce
(`tests/units/test_type_homes.py`):

1. **Cross-layer contracts**, used by two layers or more: `core.contract`.
   Generic value types (`JsonValue`, `JsonObject`) are in `shared.utils`.
2. **A package's own**, in one file of each kind: `documents.py` for the
   shapes it stores or serializes (MongoDB documents, the compiled
   mappings' JSON), `clients.py` for what it uses of a library's objects
   (Google, Azure, confluent-kafka, openpyxl), `types.py` for anything else
   its modules share or its callers pass. Each lists its names in
   `__all__`, which is what this page shows.
3. **A module's own** `type` aliases stay in it, private (`_Name`).

A package's files of the second tier are its own: used by its modules, the
composition root (`app`), the benchmarks and the tests. A type another
package uses moves to `core.contract` (the attributes of metrics and traces,
the verdict of a cutover report); two stay, listed with their reason in
`CROSS_PACKAGE_TYPES`: `core.runtime.types.PipelineFactory` (it names the
runtime's `Resources`) and `core.operators.types.SourceRecord` (the
operators' rows, which the mapping pipelines correct).

A `TypedDict` or a `Protocol` defined anywhere else, or a public `type` alias
out of these files, fails the tests with the file to move it to. Two stay in
their module, as moving them would make an import cycle:
`core.runtime.resources.Declaration` and `core.cutover.checksum.Canonical`.

Stored values are a contract too: the values of the enums and `Literal`s a
ledger, a store, a catalog or a report holds are recorded in
`tests/units/core/test_stored_enums.py`.

## Contracts

```{eval-rst}
.. automodule:: core.contract
   :members:
   :undoc-members:
   :no-index:
```

## Core

```{eval-rst}
.. automodule:: core.mapping.documents
   :undoc-members:
   :no-index:
.. automodule:: core.mapping.types
   :undoc-members:
   :no-index:
.. automodule:: core.mapping_pipelines.types
   :undoc-members:
   :no-index:
.. automodule:: core.operators.types
   :undoc-members:
   :no-index:
.. automodule:: core.runtime.types
   :undoc-members:
   :no-index:
```

## Adapters

```{eval-rst}
.. automodule:: infra.database.mongodb.documents
   :undoc-members:
   :no-index:
.. automodule:: infra.database.relational.types
   :undoc-members:
   :no-index:
.. automodule:: infra.http.types
   :undoc-members:
   :no-index:
.. automodule:: infra.mapping.clients
   :undoc-members:
   :no-index:
.. automodule:: infra.messaging.clients
   :undoc-members:
   :no-index:
.. automodule:: infra.messaging.types
   :undoc-members:
   :no-index:
.. automodule:: infra.storage.clients
   :undoc-members:
   :no-index:
```

## Composition root

```{eval-rst}
.. automodule:: app.clients
   :undoc-members:
   :no-index:
.. automodule:: app.types
   :undoc-members:
   :no-index:
```
