"""CLI entry point for datanomy."""

import sys
from pathlib import Path
from typing import Optional

import click

from datanomy.reader.parquet import ParquetReader
from datanomy.reader.s3_parquet import S3ParquetReader
from datanomy.tui.tui import DatanomyApp


@click.command()
@click.argument("file", type=click.Path(path_type=str))
@click.option("--access-key-id", help="AWS Access Key ID for S3.")
@click.option("--secret-access-key", help="AWS Secret Access Key for S3.")
@click.option("--endpoint-url", help="S3 endpoint URL for S3-compatible storage.")
def main(
    file: str,
    access_key_id: Optional[str],
    secret_access_key: Optional[str],
    endpoint_url: Optional[str],
) -> None:
    """
    Explore the anatomy of your Parquet files from local or S3.

    For S3, provide the file as an S3 URI (e.g., s3://bucket/file.parquet)
    and the required credentials.
    """
    try:
        reader: ParquetReader | S3ParquetReader
        is_s3 = file.startswith("s3://") or (access_key_id and secret_access_key)

        if is_s3:
            # if not file.startswith("s3://"):
            #     click.echo("Error: For S3 access, file path must be an S3 URI (s3://...).", err=True)
            #     sys.exit(1)
            if not access_key_id or not secret_access_key:
                click.echo("Error: S3 access requires --access-key-id and --secret-access-key.", err=True)
                sys.exit(1)
            reader = S3ParquetReader(
                file_uri=file,
                access_key_id=access_key_id,
                secret_access_key=secret_access_key,
                endpoint_url=endpoint_url,
            )
        else:
            reader = ParquetReader(Path(file))

        app = DatanomyApp(reader)
        app.run()
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
