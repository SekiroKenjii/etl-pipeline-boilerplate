# Mappings, for business analysts

A mapping says how a legacy table becomes a target table, column by column.
Mappings live in Excel workbooks (`mappings/<domain>.xlsx`), which business
analysts write and developers review as the JSON they compile to
(`mappings/compiled/<domain>.json`).

```sh
uv run etl-mappings template mappings/billing.xlsx   # a workbook to start
uv run etl-mappings validate                         # every problem, at once
uv run etl-mappings compile                          # the JSON that runs
uv run etl-mappings diff mappings/compiled/crm.json mappings/crm.xlsx
```

The template's two example mappings target the workbook's module
(`billing.customer`, `billing.order` for `billing.xlsx`; `--module` names
another): a target table names the entity of its surrogate keys, which the
key crosswalk shares between modules. Replace them with your own.

`etl-mappings diff` shows what a change of the workbook does before it is
compiled:

- the fields changed;
- the lookups changed;
- the mappings whose surrogate keys change, and why (rows loaded already
  will not match): a new key version, key hashing, key case, natural key,
  key namespace, or a renamed source or target table;
- the mappings that reference a changed one.

`etl-mappings validate`, `compile` and `check` also print warnings, which
do not fail them: draft mappings, and dates read with 2-digit years.

## The sheets

The **Mappings** sheet has one row per mapping:

- source table and target table;
- status: `draft` or `approved`. Draft mappings do not run: the pipelines
  skip them, with a warning, and the mappings referencing them too (their
  foreign keys would point to rows never loaded). The other mappings run;
- the source columns left out on purpose (**Ignored source fields**);
- the policy for the others (**Unmapped source fields**: `error` or
  `ignore`);
- a **Row filter** or a **Row split**, written by a developer;
- how keys are made: **Key version**, **Key hashing**, **Key case**
  (`preserve`, `upper`, `lower`) and **Key namespace**: see
  [Keys](#keys).

**One sheet per mapping** has one row per target column:

| Column | What it holds |
| --- | --- |
| Target column, Target type | The column, and its type as the database writes it: `VARCHAR(200)`, `NUMBER(12,2)`, `DATE`... |
| Required | `Y` or `N`: an empty value rejects the row |
| Source columns | One, or several separated by commas; `address.city` in a document |
| Transform, Parameters | `concat` with `separator=" "`, `value_map` with `A=active; I=inactive`, `lookup` with `table=countries`... Parameters are separated by `;`; a value in quotes keeps its spaces and may hold `;` (`separator=";"`), `""` is a quote in it |
| Format | How dates are written in the source: `dd/MM/yyyy`. Only `yyyy`, `yy`, `MM`, `dd`, `HH`, `mm` (minutes) and `ss` are understood, case matters; put other letters in quotes (`yyyy-MM-dd'T'HH:mm`). `yy` reads 69-99 as 1969-1999 and 00-68 as 2000-2068 |
| Default | The value of an empty result |
| Key | `PK` on the surrogate key: its source columns are the natural key |
| References | The mapping a foreign key points to |
| Personal data | `name`, `email`, `phone`, `address`, `date`, `mask` or `hash`: anonymized in UAT. The target type must hold the fakes (text of 15 characters at least for `name`, 41 for `email`, 20 for `phone`, 29 for `address`, 16 for `hash`; text for `mask`; a date for `date`). The rejects of these columns never show their value |

**`lookup_<name>` sheets** translate values: key in column A, value in
column B. A large or changing lookup can stay in a database table instead.
Keep its sheet, with the header only, and name the table in the module's
settings:

```toml
[modules.crm.settings.lookups.countries]
table = "REF_COUNTRY"
key = "CODE"
value = "ISO2"
```

## Keys

A surrogate key is derived from the natural key (the source columns of the
`PK` column), in a namespace:

- **Key namespace**: empty, it is `<source table>:<target table>`, so
  renaming either table changes every key of the mapping, and the keys of
  the mappings referencing it. Before renaming a table, write the old
  `<source table>:<target table>` here (`legacy.CUST_MASTER:crm.customer`):
  the keys stay as they are. `etl-mappings diff` says which name to write.
- **Key version**: bump it only to change every key on purpose.
- **Key hashing**: empty, keys are HMACs keyed by the key secret wherever
  one is configured (`key_secret`), and public UUIDv5 elsewhere. `hmac`
  always needs the secret. `uuid5` is refused for a natural key that a
  personal column reads (a national id, a phone number): its keys could be
  computed back from the small space of such values.
- **Key case**: `upper` or `lower` for a source ignoring case, so that
  `c001` and `C001` stay one key.

Changing any of them, the natural key columns, or the key secret, makes
other keys: the rows loaded before no longer match. The run ledger records
what keys are made from (`keys=` in `etl runs`), and `etl run` refuses to
run once it changed since the last successful run, unless `--rekey`: run
`etl-mappings diff` before compiling to see which mappings it hits, and
`etl run <pipeline> --rekey` only once the target is ready for new keys
(emptied, or a new one). Rotating the key secret is such a change, for
every `hmac` mapping: see [Security](security.md#personal-data) for the
procedure.

## When rows are rejected

A row that cannot be mapped is rejected into the quarantine, with why. The
load goes on without it. Then there are three ways out:

1. **Fix the cause.** Fix the source, the mapping or a lookup, then replay
   the rows: `etl run crm.replay`.
2. **Correct the row.** When the source cannot be fixed, correct the row in
   the corrections file, which is reviewed like the mappings:

   ```text
   mapping,source_key,CRT_DT
   customer,1003,28/02/2020
   ```

   Every load applies it before the mapping, and the cutover report agrees.
   `etl quarantine export -o rejected.csv` gives the rows to start from:
   keep its `mapping` and `source_key` columns, add the source columns to
   correct. Its other columns start with `_` (`_issues`, `_record`...) and
   are not corrections, so a source column may be called `status` or `id`.
   The `source_key` is the natural key as canonical text: trimmed, numbers
   without trailing zeros (`1003`, not `1003.0`), in the mapping's key case
   (`C001`), components joined by `|`.
3. **Close the row.** When it will never be loaded: `etl quarantine close
   <id>`. For the cutover, the BAs also list the rejected rows they accept
   to leave behind, by entity and reason, in a CSV kept with the sign-off:
   see {doc}`cutover-runbook`.
