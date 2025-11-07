"""Tests for the TUI module.

These are smoke tests to ensure the UI doesn't crash.
We don't test specific text/formatting as that's brittle and changes often.
"""

from pathlib import Path

import pytest

from datanomy.reader import ParquetReader
from datanomy.tui import DatanomyApp


@pytest.mark.asyncio
async def test_app_launches_without_crash(simple_parquet: Path) -> None:
    """Test that app launches and runs without crashing."""
    reader = ParquetReader(simple_parquet)
    app = DatanomyApp(reader)

    async with app.run_test():
        # If we get here, app launched successfully
        assert app is not None


@pytest.mark.asyncio
async def test_app_has_required_widgets(simple_parquet: Path) -> None:
    """Test that all expected widgets are present."""
    reader = ParquetReader(simple_parquet)
    app = DatanomyApp(reader)

    async with app.run_test():
        # Verify core widgets exist
        assert app.query_one("#file-info") is not None
        assert app.query_one("#schema") is not None
        assert app.query_one("#row-groups") is not None


@pytest.mark.asyncio
async def test_widgets_render_without_error(simple_parquet: Path) -> None:
    """Test that all widgets can render without throwing exceptions."""
    reader = ParquetReader(simple_parquet)
    app = DatanomyApp(reader)

    async with app.run_test():
        # Call render on each widget - will raise if there's an error
        file_info = app.query_one("#file-info")
        file_info.render()

        schema = app.query_one("#schema")
        schema.render()

        row_groups = app.query_one("#row-groups")
        row_groups.render()


@pytest.mark.asyncio
async def test_app_with_empty_file(empty_parquet: Path) -> None:
    """Test that app handles empty Parquet files."""
    reader = ParquetReader(empty_parquet)
    app = DatanomyApp(reader)

    async with app.run_test():
        # Should not crash with empty file
        app.query_one("#file-info").render()
        app.query_one("#schema").render()
        app.query_one("#row-groups").render()


@pytest.mark.asyncio
async def test_app_with_complex_schema(complex_schema_parquet: Path) -> None:
    """Test that app handles complex nested schemas."""
    reader = ParquetReader(complex_schema_parquet)
    app = DatanomyApp(reader)

    async with app.run_test():
        # Should handle nested types without crashing
        app.query_one("#schema").render()


@pytest.mark.asyncio
async def test_app_with_many_columns(large_schema_parquet: Path) -> None:
    """Test that app handles files with many columns."""
    reader = ParquetReader(large_schema_parquet)
    app = DatanomyApp(reader)

    async with app.run_test():
        # Should handle large schema without crashing
        app.query_one("#schema").render()


@pytest.mark.asyncio
async def test_app_with_multiple_row_groups(multi_row_group_parquet: Path) -> None:
    """Test that app handles multiple row groups."""
    reader = ParquetReader(multi_row_group_parquet)
    app = DatanomyApp(reader)

    async with app.run_test():
        # Should handle multiple row groups without crashing
        app.query_one("#row-groups").render()
