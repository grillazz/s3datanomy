"""Parquet file reader."""

from pathlib import Path
from typing import Any

import pyarrow.parquet as pq


class ParquetReader:
    """Main class to read and inspect Parquet files."""

    def __init__(self, file_path: Path) -> None:
        """
        Initialize the Parquet reader.

        Parameters
        ----------
            file_path: Path to the Parquet file
        """
        self.file_path = file_path
        self.parquet_file = pq.ParquetFile(file_path)

    @property
    def schema_arrow(self) -> Any:
        """
        Get the Arrow schema.

        Returns
        -------
            Arrow schema for the Parquet file
        """
        return self.parquet_file.schema_arrow

    @property
    def schema_parquet(self) -> Any:
        """
        Get the Parquet schema.

        Returns
        -------
            Parquet schema for the Parquet file
        """
        return self.parquet_file.schema

    @property
    def metadata(self) -> Any:
        """
        Get file metadata.

        Returns
        -------
            File metadata
        """
        return self.parquet_file.metadata

    @property
    def num_row_groups(self) -> int:
        """
        Get number of row groups.

        Returns
        -------
            Number of row groups in the Parquet file
        """
        return int(self.parquet_file.num_row_groups)

    @property
    def num_rows(self) -> int:
        """
        Get total number of rows.

        Returns
        -------
            Total number of rows in the Parquet file
        """
        return int(self.parquet_file.metadata.num_rows)

    @property
    def file_size(self) -> int:
        """
        Get file size in bytes.

        Returns
        -------
            File size in bytes
        """
        return int(self.file_path.stat().st_size)

    def get_row_group_info(self, index: int) -> Any:
        """
        Get information about a specific row group.

        Parameters
        ----------
            index: Row group index

        Returns
        -------
            Row group metadata
        """
        return self.parquet_file.metadata.row_group(index)

    @property
    def metadata_size(self) -> int:
        """
        Get the size of the serialized footer metadata in bytes.

        Returns
        -------
            Footer metadata size in bytes
        """
        return int(self.parquet_file.metadata.serialized_size)
