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

    @property
    def page_index_size(self) -> int:
        """
        Get the size of page indexes (Column Index + Offset Index) in bytes.

        Page indexes are written between row group data and footer metadata.

        Returns
        -------
            Page index size in bytes
        """
        if self.num_row_groups == 0:
            return 0

        # Find where the last column data ends
        last_rg = self.get_row_group_info(self.num_row_groups - 1)
        last_col = last_rg.column(last_rg.num_columns - 1)
        last_data_offset = int(
            last_col.data_page_offset + last_col.total_compressed_size
        )

        # Footer starts at: file_size - metadata_size - 4 (footer size) - 4 (PAR1 magic)
        footer_start = self.file_size - self.metadata_size - 8

        # Page indexes are in the gap between last data and footer
        return footer_start - last_data_offset
