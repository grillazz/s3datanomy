from datanomy.tui import DatanomyApp

from pathlib import Path
import pytest
from datanomy.reader import ParquetReader
from textual.containers import Container, VerticalScroll
from rich.console import Console


@pytest.mark.asyncio
async def test_containers_with_simple_parquet(
    simple_parquet: Path,
) -> None:
    reader = ParquetReader(simple_parquet)
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
        assert "File: simple.parquet" in file_info
        assert "Size: 0.00 MB" in file_info
        assert "Rows: 5" in file_info
        assert "Row Groups: 1" in file_info
        schema_widget = (
            app.query_one(VerticalScroll).query_one(Container).query_one("#schema")
        )
        with console.capture() as capture:
            console.print(schema_widget.render())
        schema_info = capture.get()
        assert "id: int64" in schema_info
        assert "name: string" in schema_info
        assert "age: int64" in schema_info
        assert "score: double" in schema_info
        row_groups_widget = (
            app.query_one(VerticalScroll).query_one(Container).query_one("#row-groups")
        )
        with console.capture() as capture:
            console.print(row_groups_widget.render())
        row_groups_info = capture.get()
        assert "Row Group 0: 5 rows, 0.00 MB" in row_groups_info


@pytest.mark.asyncio
async def test_containers_with_multi_row_group_parquet(
    multi_row_group_parquet: Path,
) -> None:
    reader = ParquetReader(multi_row_group_parquet)
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
        assert "File: multi_row_group.parquet" in file_info
        assert "Size: 0.11 MB" in file_info
        assert "Rows: 10,000" in file_info
        assert "Row Groups: 5" in file_info
        schema_widget = (
            app.query_one(VerticalScroll).query_one(Container).query_one("#schema")
        )
        with console.capture() as capture:
            console.print(schema_widget.render())
        schema_info = capture.get()
        assert "id: int64" in schema_info
        assert "category: string" in schema_info
        assert "value: int64" in schema_info
        row_groups_widget = (
            app.query_one(VerticalScroll).query_one(Container).query_one("#row-groups")
        )
        with console.capture() as capture:
            console.print(row_groups_widget.render())
        row_groups_info = capture.get()
        for i in range(5):
            assert f"Row Group {i}: 2,000 rows, 0.04 MB" in row_groups_info


@pytest.mark.asyncio
async def test_containers_with_complex_schema_parquet(
    complex_schema_parquet: Path,
) -> None:
    reader = ParquetReader(complex_schema_parquet)
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
        assert "File: complex.parquet" in file_info
        assert "Size: 0.00 MB" in file_info
        assert "Rows: 3" in file_info
        assert "Row Groups: 1" in file_info
        schema_widget = (
            app.query_one(VerticalScroll).query_one(Container).query_one("#schema")
        )
        with console.capture() as capture:
            console.print(schema_widget.render())
        schema_info = capture.get()
        assert "id: int64" in schema_info
        assert "data: struct<x: int64, y: int64>" in schema_info
        assert "tags: list<element: string>" in schema_info
        row_groups_widget = (
            app.query_one(VerticalScroll).query_one(Container).query_one("#row-groups")
        )
        with console.capture() as capture:
            console.print(row_groups_widget.render())
        row_groups_info = capture.get()
        assert "Row Group 0: 3 rows, 0.00 MB" in row_groups_info


@pytest.mark.asyncio
async def test_containers_with_empty_parquet(
    empty_parquet: Path,
) -> None:
    reader = ParquetReader(empty_parquet)
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
        assert "File: empty.parquet" in file_info
        assert "Size: 0.00 MB" in file_info
        assert "Rows: 0" in file_info
        assert "Row Groups: 1" in file_info
        schema_widget = (
            app.query_one(VerticalScroll).query_one(Container).query_one("#schema")
        )
        with console.capture() as capture:
            console.print(schema_widget.render())
        schema_info = capture.get()
        assert "id: int64" in schema_info
        assert "name: string" in schema_info
        row_groups_widget = (
            app.query_one(VerticalScroll).query_one(Container).query_one("#row-groups")
        )
        with console.capture() as capture:
            console.print(row_groups_widget.render())
        row_groups_info = capture.get()
        assert "Row Group 0: 0 rows, 0.00 MB" in row_groups_info


@pytest.mark.asyncio
async def test_containers_with_large_schema_parquet(
    large_schema_parquet: Path,
) -> None:
    reader = ParquetReader(large_schema_parquet)
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
        assert "File: large_schema.parquet" in file_info
        assert "Size: 0.01 MB" in file_info
        assert "Rows: 3" in file_info
        assert "Row Groups: 1" in file_info
        schema_widget = (
            app.query_one(VerticalScroll).query_one(Container).query_one("#schema")
        )
        with console.capture() as capture:
            console.print(schema_widget.render())
        schema_info = capture.get()
        for i in range(50):
            assert f"col_{i}: int64" in schema_info
        row_groups_widget = (
            app.query_one(VerticalScroll).query_one(Container).query_one("#row-groups")
        )
        with console.capture() as capture:
            console.print(row_groups_widget.render())
        row_groups_info = capture.get()
        assert "Row Group 0: 3 rows, 0.01 MB" in row_groups_info
