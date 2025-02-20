import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pytest

from data_platform import (
    CatalogError,
    DataPlatform,
    IngestError,
    PlatformError,
    QueryError,
)


@pytest.fixture
def csv_file(tmp_path: Path) -> Path:
    source = tmp_path / "sales.csv"
    lines = ["region,amount"] + [f"north,{100 + i}" for i in range(5)]
    lines += [f"south,{50 + i}" for i in range(3)]
    source.write_text("\n".join(lines), encoding="utf-8")
    return source


@pytest.fixture
def platform():
    with DataPlatform() as dp:
        yield dp


def test_ingest_csv_registers_catalog(platform, csv_file):
    result = platform.ingest_csv(csv_file)
    assert result.rows == 8
    meta = platform.catalog.get("sales")
    assert meta.columns == ("region", "amount")


def test_ingest_missing_file_raises(platform, tmp_path):
    with pytest.raises(IngestError):
        platform.ingest_csv(tmp_path / "ghost.csv")


def test_duplicate_registration_rejected(platform, csv_file):
    platform.ingest_csv(csv_file)
    with pytest.raises(CatalogError):
        platform.ingest_csv(csv_file)


def test_reingest_replaces_table(platform, csv_file, tmp_path):
    platform.ingest_csv(csv_file)
    bigger = tmp_path / "sales2.csv"
    bigger.write_text("region,amount\nwest,10\n", encoding="utf-8")
    platform.reingest_csv(bigger, "sales")
    assert platform.catalog.get("sales").row_count == 1


def test_query_is_read_only(platform, csv_file):
    platform.ingest_csv(csv_file)
    with pytest.raises(QueryError):
        platform.query("INSERT INTO sales VALUES ('x', 1)")


def test_aggregate_summary_ranks_groups(platform, csv_file):
    platform.ingest_csv(csv_file)
    rows = platform.aggregate_summary("sales", group_column="region",
                                      metric_column="amount")
    assert rows[0]["region"] == "north"
    assert rows[0]["n"] == 5


def test_stream_rows_chunks(platform, csv_file):
    platform.ingest_csv(csv_file)
    chunks = list(platform.stream_rows("SELECT * FROM sales", chunk_size=3))
    assert sum(len(c) for c in chunks) == 8


def test_query_error_wraps_sql_failure(platform):
    with pytest.raises(QueryError):
        platform.query("SELECT * FROM missing_table")


def test_ddl_execution_works(platform):
    platform.execute_ddl("CREATE TABLE t (x INTEGER)")
    platform.query("SELECT * FROM t")


def test_save_catalog_json(platform, csv_file, tmp_path):
    platform.ingest_csv(csv_file)
    target = tmp_path / "catalog.json"
    platform.save_catalog(target)
    assert '"sales"' in target.read_text(encoding="utf-8")


def test_platform_requires_duckdb(monkeypatch):
    import builtins
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "duckdb":
            raise ImportError("blocked for test")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(PlatformError):
        DataPlatform()
