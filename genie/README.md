# Genie Space

requires

- a SQL warehouse with `CAN USE` permission
- `SELECT` privilieges on Unity Catalog data objects.

## (Genie) Knowledge store: the knowledge base
>
> Genie automatically stores example values and creates value dictionaries for eligible columns as you add tables to the space.

- Edit metadata:
  - in **Configure** > **Data**.
  - targeting
    - Descriptions for tables and columns.
      - defaults to Unity Catalog description metadata
    - column-level synonyms: comma-separated list of keywords
    - Sampled values in columns (default enabled): once enabled for table, Genie automatically take samples
    - Value dictionaries (default enabled): Curated lists of the most relevant values in a column, used to match user prompts to actual data.
      - for enum-like string column, e.g. states and product categories
      - mostly in dimension-like table
      - up to 120 columns.
      - Each dictionary column can include up to 1,024 distinct values that are less than 127 characters in length.
      - you can manually select the columns.
  - local (scoped) to space
    - It does not overwrite Unity Catalog metadata or affect other Databricks assets
    - Knowledge store details do not count toward the [instruction limit].
  - usecase: to include company-specific information relevant to how the Genie space is used.
- Provide data models: define JOIN relationships between tables.
  - relation types: `many to one` | `one to many` | `one to one`

## Trusted assets

- **verified answers to questions**: Example SQL Queries with parameters and SQL Functions that you add to a space's context (**Configure** > **Context** > **SQL Queries**) are treated as trusted assets.
- When a user submits a question that invokes a trusted asset, it's indicated in the response as a badge <img width="70" height="29" alt="image" src="https://github.com/user-attachments/assets/fa2dd2e7-08eb-4518-8409-824691f2eed8" />

## Benchmark

set test questions that you can run to assess Genie's overall response accuracy

- Each question is processed as a new query
  - They do not carry the same context as a threaded Genie conversation
