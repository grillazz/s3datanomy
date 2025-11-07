"""Shared test fixtures for datanomy tests."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


@pytest.fixture
def simple_parquet(tmp_path: Path) -> Path:
    """Create a simple test Parquet file with basic types.

    Returns:
        Path to the created Parquet file
    """
    table = pa.table(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "age": [25, 30, 35, 40, 45],
            "score": [85.5, 90.0, 78.5, 92.0, 88.5],
        }
    )
    file_path = tmp_path / "simple.parquet"
    pq.write_table(table, file_path)
    return file_path


@pytest.fixture
def multi_row_group_parquet(tmp_path: Path) -> Path:
    """Create a Parquet file with multiple row groups.

    Returns:
        Path to the created Parquet file
    """
    # Create a larger table
    num_rows = 10000
    table = pa.table(
        {
            "id": range(num_rows),
            "value": [i * 2 for i in range(num_rows)],
            "category": [f"cat_{i % 10}" for i in range(num_rows)],
        }
    )
    file_path = tmp_path / "multi_row_group.parquet"
    # Write with small row group size to create multiple row groups
    pq.write_table(table, file_path, row_group_size=2000)
    return file_path


@pytest.fixture
def complex_schema_parquet(tmp_path: Path) -> Path:
    """Create a Parquet file with complex nested schema.

    Returns:
        Path to the created Parquet file
    """
    # Create a table with nested types
    table = pa.table(
        {
            "id": [1, 2, 3],
            "data": [
                {"x": 1, "y": 2},
                {"x": 3, "y": 4},
                {"x": 5, "y": 6},
            ],
            "tags": [["a", "b"], ["c"], ["d", "e", "f"]],
        }
    )
    file_path = tmp_path / "complex.parquet"
    pq.write_table(table, file_path)
    return file_path


@pytest.fixture
def empty_parquet(tmp_path: Path) -> Path:
    """Create an empty Parquet file (schema but no rows).

    Returns:
        Path to the created Parquet file
    """
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
        ]
    )
    table = pa.table({"id": [], "name": []}, schema=schema)
    file_path = tmp_path / "empty.parquet"
    pq.write_table(table, file_path)
    return file_path


@pytest.fixture
def large_schema_parquet(tmp_path: Path) -> Path:
    """Create a Parquet file with many columns.

    Returns:
        Path to the created Parquet file
    """
    # Create 50 columns
    num_cols = 50
    data = {f"col_{i}": [i, i + 1, i + 2] for i in range(num_cols)}
    table = pa.table(data)
    file_path = tmp_path / "large_schema.parquet"
    pq.write_table(table, file_path)
    return file_path
