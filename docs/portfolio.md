# DiVA Research Lakehouse (portfolio summary)

A small, end-to-end data engineering project on Microsoft Fabric. It harvests
student-thesis metadata from DiVA, the Swedish academic archive, over OAI-PMH. The
metadata goes through a Bronze → Silver → Gold Delta Lake medallion pipeline in PySpark.

**What it shows**

* A hand-written OAI-PMH 2.0 client using only the standard library: Identify,
  ListRecords with Dublin Core, XML namespaces, resumption tokens, timeouts, bounded
  retries with exponential backoff and Retry-After, OAI error handling, deleted
  records, and polite request pacing.
* Raw responses are stored byte-for-byte. Each run has a manifest with SHA-256
  checksums and a completeness flag. A harvest truncated at its record cap never
  advances the incremental watermark.
* Bronze keeps per-run observations with full lineage back to the raw file. Broken
  records go to a quarantine table instead of being dropped.
* Silver applies documented cleaning rules and a deterministic content hash. It
  de-duplicates within the batch and applies an explicit Delta `MERGE` with
  new/changed/unchanged/deleted/stale semantics. An older observation can never
  overwrite a newer one.
* Blocking data-quality gates run before Gold tables and exports are replaced. If a
  gate fails, the last approved outputs are kept. Every run writes a report with the
  counts it actually computed.
* The Fabric deliverable is generated from one modular Python package. The notebooks
  embed the package source, so they run after a plain Git sync with no library upload
  or custom environment. CI checks that the generated notebooks match the source.
* Tests: pure-Python unit tests, plus local Spark/Delta tests for MERGE, conflicts,
  idempotent re-runs, the quality gate, and an execution of the generated notebook.
  The tests use a synthetic two-batch replay demo.

**Scope and honesty notes:** the first version uses metadata only (no full texts, no
AI features). Every Gold output is labelled as a limited sample of the harvested
interval, not as statistics for DiVA or Sweden. The pipeline has been verified
locally with Spark 3.5 and Delta 3.2. Execution in Fabric and against the live DiVA
endpoint still has to be confirmed.

**Stack:** Microsoft Fabric (Lakehouse, notebooks, Git integration), PySpark,
Delta Lake, Python standard library, pytest, GitHub Actions.
