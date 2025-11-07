"""Tests for the CLI module."""

from pathlib import Path
from unittest.mock import Mock, patch

from click.testing import CliRunner

from datanomy.cli import main


def test_cli_rejects_non_parquet_extension(tmp_path: Path) -> None:
    """Test that CLI rejects files without .parquet extension."""
    bad_file = tmp_path / "test.txt"
    bad_file.write_text("not parquet")

    runner = CliRunner()
    result = runner.invoke(main, [str(bad_file)])

    assert result.exit_code == 1
    assert "does not appear to be a Parquet file" in result.output


def test_cli_rejects_nonexistent_file() -> None:
    """Test that CLI rejects files that don't exist."""
    runner = CliRunner()
    result = runner.invoke(main, ["/nonexistent/file.parquet"])

    assert result.exit_code == 2
    assert "does not exist" in result.output.lower()


@patch("datanomy.cli.DatanomyApp")
def test_cli_launches_app_with_valid_file(mock_app: Mock, simple_parquet: Path) -> None:
    """Test that CLI launches the app with a valid Parquet file."""
    # Mock the app to avoid actually running the TUI
    mock_app_instance = Mock()
    mock_app.return_value = mock_app_instance

    runner = CliRunner()
    result = runner.invoke(main, [str(simple_parquet)])

    # Should have created an app instance
    assert mock_app.called
    # Should have called run on the app
    assert mock_app_instance.run.called
    # Should exit successfully
    assert result.exit_code == 0


@patch("datanomy.cli.DatanomyApp")
@patch("datanomy.cli.ParquetReader")
def test_cli_creates_reader(
    mock_reader: Mock, mock_app: Mock, simple_parquet: Path
) -> None:
    """Test that CLI creates a ParquetReader with the correct file path."""
    runner = CliRunner()
    runner.invoke(main, [str(simple_parquet)])

    # Should have created a reader with the file path
    mock_reader.assert_called_once_with(simple_parquet)


def test_cli_case_insensitive_extension(tmp_path: Path) -> None:
    """Test that CLI accepts .PARQUET extension (case insensitive)."""
    # Create a valid parquet file with uppercase extension
    import pyarrow as pa
    import pyarrow.parquet as pq

    file_path = tmp_path / "test.PARQUET"
    table = pa.table({"id": [1, 2, 3]})
    pq.write_table(table, file_path)

    with patch("datanomy.cli.DatanomyApp"):
        runner = CliRunner()
        result = runner.invoke(main, [str(file_path)])

        # Should accept uppercase extension
        assert result.exit_code == 0
