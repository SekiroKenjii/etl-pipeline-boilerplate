# 7. A data diff on the client, through canonical values

**Status:** accepted.

## Context

- **What the BAs need.** Proof that the target holds what the source says,
  row by row, across two different databases.
- **Why the databases cannot do it.** Database checksums do not compare
  across vendors.

## Decision

1. **Expected rows.** Source rows go through the module's mapping, without
   loading anything, into the target rows expected.
2. **Canonical form.** Both sides are compared in a canonical form:
   `Decimal("120.50")` and `120.5`, a `UUID` and its text, an aware time and
   UTC all compare equal. A number and its text never do (`Decimal("1.5")`
   and `"1.5"`, `NaN` and `"NaN"`): a column loaded as text where a number
   was expected is a difference. The form has a version
   (`checksum.FORM`, now 2), recorded in each report's provenance
   (`canonical_form`), since a new form can change which rows match.
3. **Buckets.** Digests are summed per bucket of keys. Only the buckets that
   differ are read again, row by row.

## Consequences

- **What it buys.** It is exact, it works across databases, and memory is
  bounded.
- **What it costs.** Both sides are read in full once. For very large
  tables, `--counts-only` counts in the databases first.
