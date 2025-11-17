"""S3 Parquet file reader."""

from typing import Any, NamedTuple, Optional

import obstore as obs
import pyarrow as pa
import pyarrow.parquet as pq
from obstore.store import S3Store
from pyarrow.lib import ArrowInvalid


class RowGroupSize(NamedTuple):
    """Size information for a row group."""

    compressed: int
    uncompressed: int


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
    def total_sizes(self) -> RowGroupSize:
        """
        Get total compressed and uncompressed size of the row group in bytes.

        Returns
        -------
            RowGroupSize named tuple with compressed and uncompressed sizes
        """
        compressed_sum = sum(
            self.column(j).total_compressed_size for j in range(self.num_columns)
        )
        uncompressed_sum = sum(
            self.column(j).total_uncompressed_size for j in range(self.num_columns)
        )
        return RowGroupSize(compressed=compressed_sum, uncompressed=uncompressed_sum)


class S3ParquetReader:
    """Main class to read and inspect Parquet files from S3."""

    def __init__(
        self,
        file_uri: str,
        access_key_id: str,
        secret_access_key: str,
        endpoint_url: Optional[str] = None,
    ) -> None:
        """
        Initialize the S3 Parquet reader.

        Parameters
        ----------
            file_uri: S3 URI to the Parquet file (e.g., s3://bucket/key)
            access_key_id: AWS access key ID
            secret_access_key: AWS secret access key
            endpoint_url: S3 endpoint URL for S3-compatible storage

        Raises
        ------
            ArrowInvalid: If the file is not a valid Parquet file
        """
        self.file_path = file_uri
        self.store = S3Store(
            "de-chl",
            endpoint="http://localhost:9000",
            access_key_id="minio",
            secret_access_key="minio123",
            virtual_hosted_style_request=False,
            client_options={"allow_http": True},
        )

        try:
            file_content = obs.get(self.store, file_uri).bytes()
            self._file_size = len(file_content)
            self.parquet_file = pq.ParquetFile(pa.BufferReader(file_content))
        except ArrowInvalid as e:
            raise ArrowInvalid(
                f"{file_uri} does not appear to be a Parquet file"
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
        return self._file_size

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

        last_rg = self.get_row_group_info(self.num_row_groups - 1)
        last_col = last_rg.column(last_rg.num_columns - 1)
        last_data_offset = int(
            last_col.data_page_offset + last_col.total_compressed_size
        )

        footer_start = self.file_size - self.metadata_size - 8

        return footer_start - last_data_offset
