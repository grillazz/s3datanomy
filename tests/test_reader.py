"""Tests for the ParquetReader module."""

from pathlib import Path

from datanomy.reader import ParquetReader


def test_reader_simple_file(simple_parquet: Path) -> None:
    """Test reader with a simple Parquet file."""
    reader = ParquetReader(simple_parquet)

    assert reader.file_path == simple_parquet
    assert reader.file_path.exists()

    assert reader.num_rows == 5
    assert reader.num_row_groups == 1
    assert len(reader.schema_arrow) == 4
    assert reader.file_size > 0

    metadata = reader.metadata
    assert metadata is not None
    assert metadata.num_rows == 5
    assert metadata.num_row_groups == 1


def test_reader_multi_row_groups(multi_row_group_parquet: Path) -> None:
    """Test reader with multiple row groups."""
    reader = ParquetReader(multi_row_group_parquet)

    assert reader.num_rows == 10000
    assert reader.num_row_groups == 5

    # Check each row group
    for i in range(reader.num_row_groups):
        rg = reader.get_row_group_info(i)
        assert rg.num_rows == 2000
        assert rg.total_byte_size > 0


def test_reader_empty_file(empty_parquet: Path) -> None:
    """Test reader with empty Parquet file."""
    reader = ParquetReader(empty_parquet)

    # file has schema but no rows
    assert reader.num_rows == 0
    assert reader.num_row_groups == 1
    assert len(reader.schema_arrow) == 2
    assert reader.file_size > 0


def test_reader_large_schema(large_schema_parquet: Path) -> None:
    """Test reader with many columns."""
    reader = ParquetReader(large_schema_parquet)

    assert reader.num_rows == 3
    assert len(reader.schema_arrow) == 50

    # Check all columns are named correctly
    field_names = [field.name for field in reader.schema_arrow]
    for i in range(50):
        assert f"col_{i}" in field_names
