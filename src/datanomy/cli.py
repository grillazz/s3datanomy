"""CLI entry point for datanomy."""

import sys
from pathlib import Path

import click

from datanomy.reader.parquet import ParquetReader
from datanomy.tui.tui import DatanomyApp


@click.command()
@click.argument("file", type=click.Path(exists=True, path_type=Path))
def main(file: Path) -> None:
    """
    Explore the anatomy of your Parquet files.

    Parameters
    ----------
        file: Path to a Parquet file to inspect
    """
    if not file.suffix.lower() == ".parquet":
        click.echo(f"Error: {file} does not appear to be a Parquet file", err=True)
        sys.exit(1)

    try:
        reader = ParquetReader(file)
        app = DatanomyApp(reader)
        app.run()
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
