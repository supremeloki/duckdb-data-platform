from __future__ import annotations

import csv
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Sequence


class PlatformError(Exception):
    pass


class IngestError(PlatformError):
    pass


class CatalogError(PlatformError):
    pass


class QueryError(PlatformError):
    pass


@dataclass(frozen=True)
class TableMeta:
    name: str
    source_path: str
    row_count: int
    columns: tuple[str, ...]
    ingested_at: float = field(default_factory=time.time)

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "source": self.source_path,
            "rows": self.row_count,
            "columns": list(self.columns),
        }


@dataclass(frozen=True)
class IngestResult:
    table: str
    rows: int
    duration_ms: float


@dataclass(frozen=True)
class QueryResult:
    sql: str
    rows: list[dict[str, Any]]
    row_count: int
    duration_ms: float


class Catalog:
    def __init__(self) -> None:
        self._tables: dict[str, TableMeta] = {}

    def register(self, meta: TableMeta) -> None:
        if meta.name in self._tables:
            raise CatalogError(f"table already registered: {meta.name!r}")
        self._tables[meta.name] = meta

    def replace(self, meta: TableMeta) -> None:
        if meta.name not in self._tables:
            raise CatalogError(f"unknown table: {meta.name!r}")
        self._tables[meta.name] = meta

    def get(self, name: str) -> TableMeta:
        meta = self._tables.get(name)
        if meta is None:
            raise CatalogError(f"unknown table: {name!r}")
        return meta

    def all_tables(self) -> tuple[TableMeta, ...]:
        return tuple(sorted(self._tables.values(), key=lambda m: m.name))

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = [meta.as_dict() for meta in self.all_tables()]
        with path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False)


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


class DataPlatform:
    def __init__(self, database: str = ":memory:") -> None:
        try:
            import duckdb
        except ImportError as exc:
            raise PlatformError("duckdb is required for the platform runtime") from exc
        self._duckdb = duckdb
        self._conn = duckdb.connect(database)
        self.catalog = Catalog()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "DataPlatform":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def ingest_csv(
        self,
        source: Path,
        table_name: str | None = None,
        batch_size: int = 10_000,
    ) -> IngestResult:
        started = time.perf_counter()
        name = table_name or source.stem.lower().replace("-", "_")
        if not source.exists():
            raise IngestError(f"source file missing: {source}")
        try:
            with source.open(encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                if reader.fieldnames is None:
                    raise IngestError(f"empty CSV: {source.name}")
                columns = list(reader.fieldnames)
                column_defs = ", ".join(_quote(c) + " VARCHAR" for c in columns)
                self._conn.execute(f"CREATE OR REPLACE TABLE {_quote(name)} ({column_defs})")
                insert_sql = (
                    f"INSERT INTO {_quote(name)} "
                    f"VALUES ({', '.join('?' for _ in columns)})"
                )
                total = 0
                while True:
                    batch = []
                    for _ in range(batch_size):
                        record = next(reader, None)
                        if record is None:
                            break
                        batch.append([record.get(c) or "" for c in columns])
                    if not batch:
                        break
                    self._conn.executemany(insert_sql, batch)
                    total += len(batch)
        except OSError as exc:
            raise IngestError(f"cannot read {source.name}: {exc}") from exc
        except csv.Error as exc:
            raise IngestError(f"malformed CSV {source.name}: {exc}") from exc
        duration = (time.perf_counter() - started) * 1000
        self.catalog.register(TableMeta(
            name=name, source_path=str(source), row_count=total,
            columns=tuple(columns),
        ))
        return IngestResult(table=name, rows=total, duration_ms=round(duration, 3))

    def reingest_csv(self, source: Path, table_name: str) -> IngestResult:
        self.catalog.get(table_name)
        self.catalog._tables.pop(table_name)
        return self.ingest_csv(source, table_name=table_name)

    def query(self, sql: str, params: Sequence[Any] = ()) -> QueryResult:
        if any(word in sql.lower().split() for word in ("insert", "update", "delete", "drop")):
            raise QueryError("query() is read-only; use execute_ddl()")
        started = time.perf_counter()
        try:
            cursor = self._conn.execute(sql, list(params))
            columns = [d[0] for d in cursor.description] if cursor.description else []
            raw_rows = cursor.fetchall()
        except self._duckdb.Error as exc:
            raise QueryError(f"SQL failed: {exc}") from exc
        duration = (time.perf_counter() - started) * 1000
        rows = [dict(zip(columns, row)) for row in raw_rows]
        return QueryResult(sql=sql, rows=rows, row_count=len(rows),
                           duration_ms=round(duration, 3))

    def execute_ddl(self, sql: str) -> None:
        try:
            self._conn.execute(sql)
        except self._duckdb.Error as exc:
            raise QueryError(f"DDL failed: {exc}") from exc

    def stream_rows(self, sql: str, chunk_size: int = 50_000) -> Iterator[list[tuple]]:
        cursor = self._conn.execute(sql)
        while True:
            chunk = cursor.fetchmany(chunk_size)
            if not chunk:
                return
            yield chunk

    def aggregate_summary(self, table_name: str, group_column: str,
                          metric_column: str) -> list[dict[str, Any]]:
        self.catalog.get(table_name)
        cast_metric = f"TRY_CAST({_quote(metric_column)} AS DOUBLE)"
        result = self.query(
            f"SELECT {_quote(group_column)}, COUNT(*) AS n, "
            f"SUM({cast_metric}) AS total "
            f"FROM {_quote(table_name)} GROUP BY {_quote(group_column)} "
            f"ORDER BY total DESC"
        )
        return result.rows

    def save_catalog(self, path: Path) -> None:
        self.catalog.save(path)
