# Introduction

## Why `sparkrawl`?

In modern data workflows,
much of the tooling assumes strict, often managed conventions
for the formats and layouts your data are stored in:
Delta Lake, Apache Hudi, Apache Iceberg, Presto, Trino, to name a few.
They expect clean, well-structured tables stored in standardized formats like parquet or ORC,
with explicit schema and/or partitioning metadata managed in catalogs.


Unfortunately, the real world is full of ad hoc data files in CSV, JSON, and other common formats,
and in unstructured layouts.
Moreover, key metadata is frequently encoded in directory hierarchies, filenames, or auxiliary files.
Such layout-embedded metadata is essential for correctly interpreting and processing the data, 
yet is invisible to conventional tools without explicit extraction or strict adherence to conventions like Hive's.
Translating these messy data into the structures required by popular frameworks can be tedious, expensive, and error-prone.
Agile teams can't always wait for this process to catch up to their latest projects.
Therefore, many data processing workflows, e.g., written in Spark, must handle such messy, heterogeneous data.

`sparkrawl` is a micro-framework providing a declarative, modular, and portable way to define crawlers for custom layouts and their embedded semantics.
Compatible with distributed data processing frameworks like Spark,
it can make ingestion pipelines more robust, maintainable, and portable across teams and tools.

### Why is so much data outside of conventional storage?

There are many, many reasons. Maybe some of these apply to your organization..

* Agility
  * Early-stage projects or exploratory analyses often gather raw data quickly without formal schemas or cataloging.

  * Teams may prioritize speed over structure.
  Metadata might be encoded in filenames or folder structures to minimize overhead.

  * Standardizing data into formats like Parquet and managing catalogs requires upfront effort and tooling investment.
  Smaller teams or fast-moving startups may defer this until later.

  * Catalogs may quickly become incomplete or outdated.

* Technology

  * Metadata catalogs may not be fully portable.

  * Old applications or devices may dump data in proprietary or ad hoc formats (e.g., custom logs, binary dumps).

  * Scientific measurement data, or multimedia, often use specialized binary formats with embedded metadata. 
  These formats don’t map easily to tabular schemas.

* Governance

  * Data might be spread across disconnected systems without a unified ingestion process.

  * Organizations without mature data governance may have inconsistent data ingestion, leading to missing or fragmented metadata.

  * When sharing data across teams or companies,
  recipients may receive data in formats convenient to the provider, not standardized ones.
