# duckdb-data-platform

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A local lakehouse built on DuckDB: batch CSV ingestion with a metadata catalog, read-only SQL querying, streaming reads, and group-by analytics — a data platform in a single process.

## 🚀 Overview

The flagship data layer of the 2025 roadmap. `duckdb-data-platform` wraps DuckDB in platform semantics: **ingest** batches CSVs into typed tables with automatic catalog registration, the **catalog** tracks every table's source, schema, and row count as JSON-serializable metadata, `query()` is *read-only by construction* (INSERT/UPDATE/DELETE/DROP raise before touching data), and `stream_rows()` pages huge results without materializing them.

## ✨ Features

- **Batch ingestion:** chunked `executemany` inserts; malformed or missing sources raise typed errors
- **Metadata catalog:** frozen `TableMeta` per table (source, columns, row count, timestamp); JSON persistence
- **Read-only query surface:** mutating keywords rejected up front; DDL goes through an explicit gate
- **Streaming reads:** `fetchmany` chunks for result sets larger than memory
- **Group-by analytics:** `aggregate_summary` with TRY_CAST so string-typed ingests still sum correctly
- **Re-ingest support:** replace a table while keeping catalog continuity
- **Context-manager lifecycle** and graceful duckdb-missing error

## 🚧 Structure

```
duckdb-data-platform/
├── src/data_platform/
│   ├── __init__.py
│   ├── core.py
│   ├── ingest/
│   ├── lake/
│   └── query/
├── tests/
│   └── test_core.py
├── README.md
└── pyproject.toml
```

## 📦 Installation

```bash
git clone https://github.com/supremeloki/duckdb-data-platform.git
cd duckdb-data-platform
python -m venv .venv
.venv\Scripts\activate
pip install -e ".[dev]"
```

## 📋 Requirements

- Python 3.11+
- Runtime: `duckdb >= 0.9`

## 🏃 Quick Start

```python
from pathlib import Path
from data_platform import DataPlatform

with DataPlatform() as dp:
    dp.ingest_csv(Path("sales.csv"))
    top = dp.aggregate_summary("sales", group_column="region", metric_column="amount")
    print(top[0])

    rows = dp.query("SELECT * FROM sales WHERE amount > ?", [90])
    print(rows.row_count)

    dp.save_catalog(Path("catalog.json"))
```

## 🔧 Error Handling

```text
PlatformError
├── IngestError      # missing/malformed source files
├── CatalogError     # duplicate registration, unknown table
└── QueryError       # mutating SQL on query(), SQL failures, DDL errors
```

## 🧪 Testing

```bash
pytest tests/ -v
```

## 📝 Code Quality

- Full type hints (`X | None` style), frozen contracts
- Zero comments — names carry the meaning
- Read-only enforcement tested at keyword level

## 📄 License

MIT — see [LICENSE](LICENSE).

## 👤 Author

**Kooroush Masoumi**

---

⭐ Star this repo if you find it useful!
