"""Parquet file reader."""

from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from pyarrow.lib import ArrowInvalid


class RowGroup:
    """Class to represent a Parquet row group."""

    def __init__(self, row_group_metadata: Any) -> None:
        """
        Initialize the RowGroup.

        Parameters
        ----------
            row_group_metadata: Metadata for the row group
        """
        self._metadata = row_group_metadata

    @property
    def num_columns(self) -> int:
        """
        Get number of columns in the row group.

        Returns
        -------
            Number of columns
        """
        return int(self._metadata.num_columns)

    @property
    def num_rows(self) -> int:
        """
        Get number of rows in the row group.

        Returns
        -------
            Number of rows
        """
        return int(self._metadata.num_rows)

    def column(self, index: int) -> Any:
        """
        Get metadata for a specific column in the row group.

        Parameters
        ----------
            index: Column index

        Returns
        -------
            Column metadata
        """
        return self._metadata.column(index)

    @property
    def has_compression(self) -> bool:
        """
        Check if any column in the row group uses compression.

        Returns
        -------
            True if any column is compressed, False otherwise
        """
        for j in range(self.num_columns):
            if self.column(j).compression != "UNCOMPRESSED":
                return True
        return False

    @property
    def total_sizes(self) -> tuple[int, int]:
        """
        Get total compressed and uncompressed size of the row group in bytes.

        Returns
        -------
            Total compressed and uncompressed size of the row group in bytes.
            Tuple of (compressed_size, uncompressed_size)
        """
        compressed_sum = sum(
            self.column(j).total_compressed_size for j in range(self.num_columns)
        )
        # This should be self._metadata.total_byte_size but there's a bug on
        # pyarrow 22.0.0, see: https://github.com/apache/arrow/issues/48138
        uncompressed_sum = sum(
            self.column(j).total_uncompressed_size for j in range(self.num_columns)
        )
        return compressed_sum, uncompressed_sum


class ParquetReader:
    """Main class to read and inspect Parquet files."""

    def __init__(self, file_path: Path) -> None:
        """
        Initialize the Parquet reader.

        Parameters
        ----------
            file_path: Path to the Parquet file

        Raises
        ------
            FileNotFoundError: If the file does not exist
            ArrowInvalid: If the file is not a valid Parquet file
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        try:
            self.file_path = file_path
            self.parquet_file = pq.ParquetFile(file_path)
        except ArrowInvalid as e:
            raise ArrowInvalid(
                f"{file_path} does not appear to be a Parquet file"
            ) from e

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

    def get_row_group(self, index: int) -> RowGroup:
        """
        Get a specific RowGroup object.

        Parameters
        ----------
            index: Row group index

        Returns
        -------
            RowGroup object
        """
        return RowGroup(self.parquet_file.metadata.row_group(index))

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
