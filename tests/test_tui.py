"""Tests for the TUI module."""

from pathlib import Path
from typing import Any, TypedDict

import pytest
from rich.console import Console
from textual.containers import Container, VerticalScroll

from datanomy.reader import ParquetReader
from datanomy.tui import DatanomyApp


class FileDataFixture(TypedDict):
    """Type definition for test file data."""

    file_size: str
    num_rows: int
    num_row_groups: int
    schema: dict[str, str]


@pytest.fixture
def file(request: pytest.FixtureRequest) -> Any:
    """Indirect fixture to get other fixtures by name."""
    return request.getfixturevalue(request.param)


test_data_fixtures: dict[str, FileDataFixture] = {
    "simple.parquet": {
        "file_size": "0.00",
        "num_rows": 5,
        "num_row_groups": 1,
        "schema": {
            "id": "int64",
            "name": "string",
            "age": "int64",
            "score": "double",
        },
    },
    "multi_row_group.parquet": {
        "file_size": "0.11",
        "num_rows": 10000,
        "num_row_groups": 5,
        "schema": {
            "id": "int64",
            "category": "string",
            "value": "int64",
        },
    },
    "complex.parquet": {
        "file_size": "0.00",
        "num_rows": 3,
        "num_row_groups": 1,
        "schema": {
            "id": "int64",
            "data": "struct<x: int64, y: int64>",
            "tags": "list<element: string>",
        },
    },
    "empty.parquet": {
        "file_size": "0.00",
        "num_rows": 0,
        "num_row_groups": 1,
        "schema": {
            "id": "int64",
            "name": "string",
        },
    },
    "large_schema.parquet": {
        "file_size": "0.01",
        "num_rows": 3,
        "num_row_groups": 1,
        "schema": {f"col_{i}": "int64" for i in range(50)},
    },
}


async def check_app_for_file(filename: Path) -> None:
    reader = ParquetReader(filename)
    app = DatanomyApp(reader)
    async with app.run_test():
        assert app.title == "DatanomyApp"
        console = Console()
        file_info_widget = (
            app.query_one(VerticalScroll).query_one(Container).query_one("#file-info")
        )
        with console.capture() as capture:
            console.print(file_info_widget.render())
        file_info = capture.get()
        file_data = test_data_fixtures[filename.name]
        assert (
            f"File: {filename.name}\nSize: {file_data['file_size']} MB\nRows: {file_data['num_rows']:,}\nRow Groups: {file_data['num_row_groups']}"
            in file_info
        )

        schema_widget = (
            app.query_one(VerticalScroll).query_one(Container).query_one("#schema")
        )
        with console.capture() as capture:
            console.print(schema_widget.render())
        schema_info = capture.get()
        for field, dtype in file_data["schema"].items():
            assert f"{field}: {dtype}" in schema_info

        row_groups_widget = (
            app.query_one(VerticalScroll).query_one(Container).query_one("#row-groups")
        )
        with console.capture() as capture:
            console.print(row_groups_widget.render())
        row_groups_info = capture.get()
        for i in range(file_data["num_row_groups"]):
            assert (
                f"Row Group {i}: {int(file_data['num_rows']) // int(file_data['num_row_groups']):,} rows"
                in row_groups_info
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "file",
    [
        "simple_parquet",
        "multi_row_group_parquet",
        "complex_schema_parquet",
        "empty_parquet",
        "large_schema_parquet",
    ],
    indirect=True,
)
async def test_containers_with_files(
    file: Path,
) -> None:
    await check_app_for_file(file)
