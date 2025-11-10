"""Terminal UI for exploring Parquet files."""

from typing import Any

from rich.console import Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from textual.app import App, ComposeResult
from textual.containers import ScrollableContainer
from textual.widgets import Footer, Header, Static, TabbedContent, TabPane

from datanomy.reader import ParquetReader


class StructureTab(Static):
    """Widget displaying Parquet file structure."""

    def __init__(self, reader: ParquetReader) -> None:
        """
        Initialize the structure view.

        Parameters
        ----------
            reader: ParquetReader instance
        """
        super().__init__()
        self.reader = reader

    def compose(self) -> ComposeResult:
        """Render the structure view."""
        yield Static(self._render_structure(), id="structure-content")

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """
        Format size in bytes to human-readable format with byte count.

        Parameters
        ----------
            size_bytes: Size in bytes

        Returns
        -------
            str: Formatted size string (e.g., "1.23 KB (1234 bytes)")
        """
        if size_bytes < 1024:
            return f"{size_bytes} bytes"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.2f} KB ({size_bytes:,} bytes)"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.2f} MB ({size_bytes:,} bytes)"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB ({size_bytes:,} bytes)"

    def _render_structure(self) -> Group:
        """
        Render the Parquet file structure diagram.

        Returns
        -------
            Group: Rich renderable showing file structure
        """
        file_size_str = self._format_size(self.reader.file_size)

        # File info panel
        file_info = Text()
        file_info.append("File: ", style="bold")
        file_info.append(f"{self.reader.file_path.name}\n")
        file_info.append("Size: ", style="bold")
        file_info.append(file_size_str)

        # Header section
        header_text = Text()
        header_text.append("Magic Number: PAR1\n", style="yellow")
        header_text.append("Size: 4 bytes")
        header_panel = Panel(
            header_text,
            title="Header",
            border_style="yellow",
        )

        # Row groups
        row_group_panels = []
        for i in range(self.reader.num_row_groups):
            rg = self.reader.get_row_group_info(i)

            # Calculate compressed and uncompressed sizes
            compressed_sum = sum(
                rg.column(j).total_compressed_size for j in range(rg.num_columns)
            )
            uncompressed_sum = rg.total_byte_size  # This is the uncompressed size

            # Check if any column is compressed
            has_compression = any(
                rg.column(j).compression != "UNCOMPRESSED"
                for j in range(rg.num_columns)
            )

            compressed_str = self._format_size(compressed_sum)
            uncompressed_str = self._format_size(uncompressed_sum)

            # Summary info
            rg_summary = Text()
            rg_summary.append(f"Rows: {rg.num_rows:,}\n")
            if has_compression:
                rg_summary.append(f"Compressed: {compressed_str}\n")
                rg_summary.append(f"Uncompressed: {uncompressed_str}\n")
                # Calculate compression ratio
                if uncompressed_sum > 0:
                    compression_pct = (1 - compressed_sum / uncompressed_sum) * 100
                    rg_summary.append(
                        f"Compression: {compression_pct:.1f}%\n",
                        style="green" if compression_pct > 0 else "yellow",
                    )
            else:
                rg_summary.append(f"Size: {compressed_str}\n")
            rg_summary.append(f"Columns: {rg.num_columns}\n")

            # Create column chunk table
            max_cols_to_show = 20  # Limit display for files with many columns
            cols_to_display = min(rg.num_columns, max_cols_to_show)

            # Create a table with 3 columns
            col_table = Table.grid(padding=(0, 1), expand=True)
            col_table.add_column(ratio=1)
            col_table.add_column(ratio=1)
            col_table.add_column(ratio=1)

            # Build rows of column panels
            cols_per_row = 3
            for row_idx in range(0, cols_to_display, cols_per_row):
                row_panels: list[Panel | Text] = []
                for col_offset in range(cols_per_row):
                    col_idx = row_idx + col_offset
                    if col_idx < cols_to_display:
                        col = rg.column(col_idx)
                        col_compressed_str = self._format_size(
                            col.total_compressed_size
                        )
                        col_name = col.path_in_schema
                        is_compressed = col.compression != "UNCOMPRESSED"

                        col_text = Text()
                        if is_compressed:
                            col_uncompressed_str = self._format_size(
                                col.total_uncompressed_size
                            )
                            col_text.append(
                                f"Compressed: {col_compressed_str}\n", style="dim"
                            )
                            col_text.append(
                                f"Uncompressed: {col_uncompressed_str}\n", style="dim"
                            )
                            # Calculate compression ratio for this column
                            if col.total_uncompressed_size > 0:
                                col_compression_pct = (
                                    1
                                    - col.total_compressed_size
                                    / col.total_uncompressed_size
                                ) * 100
                                ratio_style = (
                                    "green" if col_compression_pct > 0 else "yellow"
                                )
                                col_text.append(
                                    f"Ratio: {col_compression_pct:.1f}%\n",
                                    style=ratio_style,
                                )
                        else:
                            col_text.append(
                                f"Size: {col_compressed_str}\n", style="dim"
                            )
                        col_text.append(f"Codec: {col.compression}\n", style="dim")
                        col_text.append(f"Type: {col.physical_type}", style="dim")

                        col_panel = Panel(
                            col_text,
                            title=f"[cyan]{col_name}[/cyan]",
                            border_style="dim",
                            padding=(0, 1),
                        )
                        row_panels.append(col_panel)
                    else:
                        # Empty space for alignment (no visible border)
                        row_panels.append(Text(""))

                col_table.add_row(*row_panels)

            # If too many columns, add note
            if rg.num_columns > max_cols_to_show:
                remaining_text = Text()
                remaining_text.append(
                    f"... and {rg.num_columns - max_cols_to_show} more columns",
                    style="dim italic",
                )
                col_table.add_row(Panel(remaining_text, border_style="dim"), "", "")

            # Combine summary and column table
            rg_content = Group(rg_summary, Text(), col_table)

            panel = Panel(
                rg_content, title=f"[green]Row Group {i}[/green]", border_style="green"
            )
            row_group_panels.append(panel)

        # Page indexes section (between row groups and footer)
        page_index_size = self.reader.page_index_size
        page_index_panels: list[Panel | Text] = []
        if page_index_size > 0:
            page_index_size_str = self._format_size(page_index_size)

            # Check what indexes and statistics are actually present
            has_col_index = False
            has_off_index = False
            has_min_max = False
            has_null_count = False
            has_distinct_count = False

            for i in range(self.reader.num_row_groups):
                rg = self.reader.get_row_group_info(i)
                for j in range(rg.num_columns):
                    col = rg.column(j)
                    if col.has_column_index:
                        has_col_index = True
                    if col.has_offset_index:
                        has_off_index = True

                    # Check what statistics are actually present
                    if col.is_stats_set:
                        stats = col.statistics
                        if stats.has_min_max:
                            has_min_max = True
                        if stats.has_null_count:
                            has_null_count = True
                        if stats.has_distinct_count:
                            has_distinct_count = True

            # Create a single parent panel if any indexes exist
            if has_col_index or has_off_index:
                page_index_content: list[Text | Table] = []

                # Total size at the top
                size_text = Text()
                size_text.append(f"Total Size: {page_index_size_str}", style="bold")
                page_index_content.append(size_text)
                page_index_content.append(Text())  # Blank line

                # Create a table for the index sub-panels
                index_table = Table.grid(padding=(0, 1), expand=True)
                index_table.add_column(ratio=1)
                index_table.add_column(ratio=1)

                index_panels: list[Panel | Text] = []

                # Column Index sub-panel
                if has_col_index:
                    col_index_text = Text()
                    col_index_text.append(
                        "Per-page statistics for filtering\n\n", style="cyan"
                    )
                    col_index_text.append("Contains:\n", style="bold")

                    # Only list statistics that are actually present
                    if has_min_max:
                        col_index_text.append(
                            "• min/max values per page\n", style="dim"
                        )
                    if has_null_count:
                        col_index_text.append("• null_count per page\n", style="dim")
                    if has_distinct_count:
                        col_index_text.append(
                            "• distinct_count per page\n", style="dim"
                        )

                    col_index_text.append("• Enables page-level pruning", style="dim")
                    col_index_panel = Panel(
                        col_index_text,
                        title="[cyan]Column Index[/cyan]",
                        border_style="dim",
                        padding=(0, 1),
                    )
                    index_panels.append(col_index_panel)
                else:
                    index_panels.append(Text(""))

                # Offset Index sub-panel
                if has_off_index:
                    offset_index_text = Text()
                    offset_index_text.append(
                        "Page locations for random access\n\n", style="cyan"
                    )
                    offset_index_text.append("Contains:\n", style="bold")
                    offset_index_text.append("• Page file offsets\n", style="dim")
                    offset_index_text.append("• compressed_page_size\n", style="dim")
                    offset_index_text.append("• first_row_index per page", style="dim")
                    offset_index_panel = Panel(
                        offset_index_text,
                        title="[cyan]Offset Index[/cyan]",
                        border_style="dim",
                        padding=(0, 1),
                    )
                    index_panels.append(offset_index_panel)
                else:
                    index_panels.append(Text(""))

                index_table.add_row(*index_panels)
                page_index_content.append(index_table)

                # Create the parent panel
                page_index_panel = Panel(
                    Group(*page_index_content),
                    title="[magenta]Page Indexes[/magenta]",
                    border_style="magenta",
                )
                page_index_panels.append(page_index_panel)

        # Footer metadata
        metadata_size_str = self._format_size(self.reader.metadata_size)

        footer_text = Text()
        footer_text.append(f"Total Rows: {self.reader.num_rows:,}\n")
        footer_text.append(f"Row Groups: {self.reader.num_row_groups}\n")
        footer_text.append(f"Metadata: {metadata_size_str}\n")
        footer_text.append("Footer Size Field: 4 bytes\n")
        footer_text.append("Magic Number: PAR1", style="yellow")
        footer_text.append(" (4 bytes)")

        footer_panel = Panel(
            footer_text, title="[blue]Footer[/blue]", border_style="blue"
        )

        # Combine all sections
        sections: list[Text | Panel] = [file_info, Text(), header_panel, Text()]
        sections.extend(row_group_panels)
        if page_index_panels:
            sections.append(Text())
            sections.extend(page_index_panels)
        sections.extend([Text(), footer_panel])

        return Group(*sections)


class SchemaTab(Static):
    """Widget displaying schema information."""

    def __init__(self, reader: ParquetReader) -> None:
        """
        Initialize the schema view.

        Parameters
        ----------
            reader: ParquetReader instance
        """
        super().__init__()
        self.reader = reader

    def compose(self) -> ComposeResult:
        """Render the schema view."""
        yield Static(self._render_schema(), id="schema-content")

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """
        Format size in bytes to human-readable format with byte count.

        Parameters
        ----------
            size_bytes: Size in bytes

        Returns
        -------
            str: Formatted size string (e.g., "1.23 KB (1234 bytes)")
        """
        if size_bytes < 1024:
            return f"{size_bytes} bytes"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.2f} KB ({size_bytes:,} bytes)"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.2f} MB ({size_bytes:,} bytes)"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB ({size_bytes:,} bytes)"

    def _render_schema(self) -> Group:
        """
        Render schema information.

        Returns
        -------
            Group: Rich renderable showing Parquet schema as column panels and structure
        """
        parquet_schema = self.reader.schema_parquet
        num_columns = len(parquet_schema.names)

        # Schema structure (Thrift-like representation)
        schema_str = str(parquet_schema)
        # Remove the first line (Python object repr)
        schema_lines = schema_str.split("\n")
        clean_schema = (
            "\n".join(schema_lines[1:]) if len(schema_lines) > 1 else schema_str
        )

        # Remove noisy field_id=-1 annotations
        clean_schema = clean_schema.replace(" field_id=-1", "")

        # Remove trailing empty lines
        clean_schema = clean_schema.rstrip()

        schema_structure_panel = Panel(
            Text(clean_schema, style="dim"),
            title="[yellow]Parquet Schema Structure[/yellow]",
            border_style="yellow",
        )

        # Create a table grid for column panels (3 columns wide)
        schema_table = Table.grid(padding=(0, 1), expand=True)
        schema_table.add_column(ratio=1)
        schema_table.add_column(ratio=1)
        schema_table.add_column(ratio=1)

        # Calculate total sizes per column across all row groups
        column_sizes: dict[
            int, tuple[int, int]
        ] = {}  # col_idx -> (compressed, uncompressed)
        for rg_idx in range(self.reader.num_row_groups):
            rg = self.reader.get_row_group_info(rg_idx)
            for col_idx in range(rg.num_columns):
                col_chunk = rg.column(col_idx)
                if col_idx not in column_sizes:
                    column_sizes[col_idx] = (0, 0)
                compressed, uncompressed = column_sizes[col_idx]
                column_sizes[col_idx] = (
                    compressed + col_chunk.total_compressed_size,
                    uncompressed + col_chunk.total_uncompressed_size,
                )

        # Build column panels
        cols_per_row = 3
        for row_idx in range(0, num_columns, cols_per_row):
            row_panels: list[Panel | Text] = []

            for col_offset in range(cols_per_row):
                col_idx = row_idx + col_offset
                if col_idx < num_columns:
                    col = parquet_schema.column(col_idx)
                    name = parquet_schema.names[col_idx]

                    # Build column info
                    col_text = Text()

                    # Show total size
                    if col_idx in column_sizes:
                        compressed, uncompressed = column_sizes[col_idx]
                        if compressed != uncompressed:
                            col_text.append("Compressed: ", style="bold")
                            col_text.append(
                                f"{self._format_size(compressed)}\n", style="dim"
                            )
                            col_text.append("Uncompressed: ", style="bold")
                            col_text.append(
                                f"{self._format_size(uncompressed)}\n", style="dim"
                            )
                            # Calculate compression ratio
                            if uncompressed > 0:
                                compression_pct = (1 - compressed / uncompressed) * 100
                                ratio_style = (
                                    "green" if compression_pct > 0 else "yellow"
                                )
                                col_text.append("Compression: ", style="bold")
                                col_text.append(
                                    f"{compression_pct:.1f}%\n", style=ratio_style
                                )
                        else:
                            col_text.append("Size: ", style="bold")
                            col_text.append(
                                f"{self._format_size(compressed)}\n", style="dim"
                            )
                        col_text.append("\n")

                    # Physical type
                    col_text.append("Physical Type: ", style="bold")
                    col_text.append(f"{col.physical_type}\n", style="yellow")

                    # Logical type (if present and meaningful)
                    if col.logical_type is not None and str(col.logical_type) != "None":
                        col_text.append("Logical Type: ", style="bold")
                        col_text.append(f"{col.logical_type}\n", style="cyan")
                    else:
                        # Add blank line for alignment
                        col_text.append("\n")

                    col_text.append("\n")

                    # Repetition level
                    col_text.append("Max Repetition Level: ", style="bold")
                    col_text.append(f"{col.max_repetition_level}\n", style="dim")
                    if col.max_repetition_level == 0:
                        col_text.append(
                            "  → Not repeated (flat value)\n", style="dim italic"
                        )
                    else:
                        col_text.append(
                            "  → Repeated (list/nested lists)\n", style="dim italic"
                        )

                    # Definition level
                    col_text.append("Max Definition Level: ", style="bold")
                    col_text.append(f"{col.max_definition_level}\n", style="dim")

                    # Explain what definition level means
                    if col.max_definition_level == 0:
                        col_text.append(
                            "  → REQUIRED (no nulls allowed)\n", style="dim italic"
                        )
                    else:
                        col_text.append(
                            "  → OPTIONAL (value can be null)\n", style="dim italic"
                        )

                    col_panel = Panel(
                        col_text,
                        title=f"[green]{name}[/green]",
                        border_style="cyan",
                        padding=(0, 1),
                    )
                    row_panels.append(col_panel)
                else:
                    # Empty space for alignment
                    row_panels.append(Text(""))

            schema_table.add_row(*row_panels)

        # Column details panel
        column_details_panel = Panel(
            schema_table,
            title="[cyan]Column Details[/cyan]",
            border_style="cyan",
        )

        return Group(schema_structure_panel, Text(), column_details_panel)


class StatsTab(Static):
    """Widget displaying column statistics."""

    def __init__(self, reader: ParquetReader) -> None:
        """
        Initialize the stats view.

        Parameters
        ----------
            reader: ParquetReader instance
        """
        super().__init__()
        self.reader = reader

    def compose(self) -> ComposeResult:
        """Render the stats view."""
        yield Static(self._render_stats(), id="stats-content")

    def _render_stats(self) -> Group:
        """
        Render column statistics.

        Returns
        -------
            Group: Rich renderable showing statistics per column
        """
        num_columns = self.reader.metadata.num_columns

        # Check if any statistics exist in the file
        has_any_stats = False
        for rg_idx in range(self.reader.num_row_groups):
            rg = self.reader.get_row_group_info(rg_idx)
            for col_idx in range(rg.num_columns):
                col = rg.column(col_idx)
                if col.is_stats_set:
                    has_any_stats = True
                    break
            if has_any_stats:
                break

        if not has_any_stats:
            no_stats_text = Text()
            no_stats_text.append(
                "No statistics found in this Parquet file.\n\n", style="yellow"
            )
            no_stats_text.append(
                "Statistics can be written during file creation using write options.",
                style="dim",
            )
            return Group(Panel(no_stats_text, title="[yellow]Statistics[/yellow]"))

        # Create a table grid for column statistics (3 columns wide)
        stats_table = Table.grid(padding=(0, 1), expand=True)
        stats_table.add_column(ratio=1, min_width=20)
        stats_table.add_column(ratio=1, min_width=20)
        stats_table.add_column(ratio=1, min_width=20)

        # Build statistics panels per column
        cols_per_row = 3
        for row_idx in range(0, num_columns, cols_per_row):
            row_panels: list[Panel | Text] = []
            row_texts: list[Text] = []  # Store texts to calculate max height

            # First pass: build all text content
            for col_offset in range(cols_per_row):
                col_idx = row_idx + col_offset
                if col_idx < num_columns:
                    col_name = self.reader.schema_parquet.names[col_idx]

                    # Collect statistics from all row groups for this column
                    col_text = Text()
                    has_stats_for_col = False

                    for rg_idx in range(self.reader.num_row_groups):
                        rg = self.reader.get_row_group_info(rg_idx)
                        col_chunk = rg.column(col_idx)

                        if col_chunk.is_stats_set:
                            has_stats_for_col = True
                            stats = col_chunk.statistics

                            # Row group header
                            if self.reader.num_row_groups > 1:
                                col_text.append(
                                    f"Row Group {rg_idx}:\n", style="bold cyan"
                                )

                            # Number of values (always present when stats are set)
                            col_text.append("  num_values: ", style="bold")
                            col_text.append(f"{stats.num_values:,}\n", style="cyan")

                            # Min/Max
                            if stats.has_min_max:
                                col_text.append("  min: ", style="bold")
                                col_text.append(f"{stats.min}\n", style="green")
                                col_text.append("  max: ", style="bold")
                                col_text.append(f"{stats.max}\n", style="green")

                            # Null count
                            if stats.has_null_count:
                                col_text.append("  null_count: ", style="bold")
                                col_text.append(
                                    f"{stats.null_count:,}\n", style="yellow"
                                )

                            # Distinct count
                            if stats.has_distinct_count:
                                col_text.append("  distinct_count: ", style="bold")
                                col_text.append(
                                    f"{stats.distinct_count:,}\n", style="magenta"
                                )

                            # Add spacing between row groups
                            if rg_idx < self.reader.num_row_groups - 1:
                                col_text.append("\n")

                    # If no statistics for this column, show message
                    if not has_stats_for_col:
                        col_text.append("No statistics available", style="dim yellow")

                    row_texts.append(col_text)
                else:
                    row_texts.append(Text(""))

            # Calculate max height (number of lines) in this row
            max_lines = max(
                len(str(t).split("\n")) for t in row_texts if str(t).strip()
            )

            # Second pass: pad texts to equal height and create panels
            for col_offset in range(cols_per_row):
                col_idx = row_idx + col_offset
                if col_idx < num_columns:
                    col_name = self.reader.schema_parquet.names[col_idx]
                    col_text = row_texts[col_offset]

                    # Pad with empty lines to match max_lines
                    current_lines = len(str(col_text).split("\n"))
                    for _ in range(max_lines - current_lines):
                        col_text.append("\n")

                    col_panel = Panel(
                        col_text,
                        title=f"[green]{col_name}[/green]",
                        border_style="cyan",
                        padding=(0, 1),
                        expand=True,
                    )
                    row_panels.append(col_panel)
                else:
                    # Empty space for alignment
                    row_panels.append(Text(""))

            stats_table.add_row(*row_panels)

        return Group(stats_table)


class DataTab(Static):
    """Widget displaying data preview."""

    def __init__(self, reader: ParquetReader, num_rows: int = 50) -> None:
        """
        Initialize the data view.

        Parameters
        ----------
            reader: ParquetReader instance
            num_rows: Number of rows to display (default: 50)
        """
        super().__init__()
        self.reader = reader
        self.num_rows = num_rows

    def compose(self) -> ComposeResult:
        """Render the data view."""
        yield Static(self._render_data(), id="data-content")

    @staticmethod
    def _format_value(value: Any, max_length: int = 50) -> str:
        """
        Format a value for display.

        Parameters
        ----------
            value: The value to format
            max_length: Maximum string length before truncation

        Returns
        -------
            str: Formatted value string
        """
        if value is None:
            return "NULL"

        value_str = str(value)
        if len(value_str) > max_length:
            return f"{value_str[: max_length - 3]}..."
        return value_str

    def _render_data(self) -> Group:
        """
        Render data preview table.

        Returns
        -------
            Group: Rich renderable showing data rows
        """
        # Read data from the Parquet file
        try:
            table = self.reader.parquet_file.read(columns=None, use_threads=True)

            # Limit to requested number of rows
            if len(table) > self.num_rows:
                table = table.slice(0, self.num_rows)

            num_rows_display = len(table)
            total_rows = self.reader.num_rows

        except Exception as e:
            error_text = Text()
            error_text.append(f"Error reading data: {e}", style="red")
            return Group(Panel(error_text, title="[red]Error[/red]"))

        # Create header info
        header_text = Text()
        header_text.append(
            f"Showing {num_rows_display:,} of {total_rows:,} rows", style="cyan bold"
        )

        # Create Rich table
        data_table = Table(
            show_header=True,
            header_style="bold cyan",
            border_style="dim",
            expand=False,
            box=None,
        )

        # Add columns
        for col_name in table.schema.names:
            data_table.add_column(col_name, style="white", no_wrap=False)

        # Add rows
        for i in range(num_rows_display):
            row_values: list[str | Text] = []
            for col_name in table.schema.names:
                value = table[col_name][i].as_py()
                formatted_value = self._format_value(value)

                # Style NULL values differently
                if value is None:
                    row_values.append(Text(formatted_value, style="dim yellow"))
                else:
                    row_values.append(formatted_value)

            data_table.add_row(*row_values)

        # Wrap table in panel
        table_panel = Panel(
            data_table,
            title="[cyan]Data Preview[/cyan]",
            border_style="cyan",
        )

        return Group(header_text, Text(), table_panel)


class MetadataTab(Static):
    """Display Parquet file metadata."""

    def __init__(self, reader: ParquetReader) -> None:
        """Initialize the metadata tab."""
        super().__init__()
        self.reader = reader

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """Format size in bytes to human-readable format."""
        if size_bytes < 1024:
            return f"{size_bytes} bytes"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.2f} KB ({size_bytes:,} bytes)"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.2f} MB ({size_bytes:,} bytes)"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB ({size_bytes:,} bytes)"

    def compose(self) -> ComposeResult:
        """Compose the metadata view."""
        yield Static(self._render_metadata(), id="metadata-content")

    def _render_metadata(self) -> Group:
        """Render file metadata."""
        metadata = self.reader.metadata

        # File information section
        file_info = Text()
        file_info.append("Created by: ", style="bold")
        file_info.append(f"{metadata.created_by}\n", style="cyan")
        file_info.append("Format version: ", style="bold")
        file_info.append(f"{metadata.format_version}\n", style="cyan")
        file_info.append("Metadata size: ", style="bold")
        file_info.append(
            f"{self._format_size(metadata.serialized_size)}\n", style="cyan"
        )
        file_info.append("\n")

        file_info.append("Total rows: ", style="bold")
        file_info.append(f"{metadata.num_rows:,}\n", style="green")
        file_info.append("Total columns: ", style="bold")
        file_info.append(f"{metadata.num_columns}\n", style="green")
        file_info.append("Row groups: ", style="bold")
        file_info.append(f"{metadata.num_row_groups}\n", style="green")

        # Calculate total compressed and uncompressed sizes
        total_compressed = 0
        total_uncompressed = 0
        for rg_idx in range(metadata.num_row_groups):
            rg = metadata.row_group(rg_idx)
            for col_idx in range(rg.num_columns):
                col = rg.column(col_idx)
                total_compressed += col.total_compressed_size
                total_uncompressed += col.total_uncompressed_size

        file_info.append("\n")
        file_info.append("Total compressed size: ", style="bold")
        file_info.append(f"{self._format_size(total_compressed)}\n", style="cyan")
        file_info.append("Total uncompressed size: ", style="bold")
        file_info.append(f"{self._format_size(total_uncompressed)}\n", style="cyan")

        if total_uncompressed > 0:
            compression_pct = (1 - total_compressed / total_uncompressed) * 100
            ratio_style = "green" if compression_pct > 0 else "yellow"
            file_info.append("Compression ratio: ", style="bold")
            file_info.append(f"{compression_pct:.1f}%\n", style=ratio_style)

        file_info_panel = Panel(
            file_info,
            title="[cyan]File Information[/cyan]",
            border_style="cyan",
        )

        # Custom metadata section
        custom_metadata = Text()
        if metadata.metadata:
            for key, value in metadata.metadata.items():
                # Keys and values are bytes
                key_str = key.decode("utf-8") if isinstance(key, bytes) else key
                value_str = value.decode("utf-8") if isinstance(value, bytes) else value

                custom_metadata.append(f"{key_str}:\n", style="bold yellow")
                # For long values like ARROW:schema, just show truncated
                if len(value_str) > 200:
                    custom_metadata.append(
                        f"  {value_str[:200]}...\n", style="dim white"
                    )
                    custom_metadata.append(
                        f"  (truncated, {len(value_str)} bytes total)\n",
                        style="italic magenta",
                    )
                else:
                    custom_metadata.append(f"  {value_str}\n", style="white")
                custom_metadata.append("\n")
        else:
            custom_metadata.append("No custom metadata found", style="dim yellow")

        custom_metadata_panel = Panel(
            custom_metadata,
            title="[cyan]Custom Metadata[/cyan]",
            border_style="cyan",
        )

        return Group(file_info_panel, Text(), custom_metadata_panel)


class DatanomyApp(App):
    """A Textual app to explore Parquet file anatomy."""

    CSS = """
    TabbedContent {
        height: 1fr;
    }

    TabPane {
        padding: 1;
    }

    #structure-content, #schema-content, #stats-content, #data-content {
        padding: 1;
    }
    """

    BINDINGS = [("q", "quit", "Quit")]

    def __init__(self, reader: ParquetReader) -> None:
        """
        Initialize the app.

        Parameters
        ----------
            reader: ParquetReader instance
        """
        super().__init__()
        self.reader = reader

    def compose(self) -> ComposeResult:
        """
        Create child widgets for the app.

        Yields
        ------
            ComposeResult: Child widgets
        """
        yield Header()
        with TabbedContent():
            with TabPane("Structure", id="tab-structure"):
                yield ScrollableContainer(StructureTab(self.reader))
            with TabPane("Schema", id="tab-schema"):
                yield ScrollableContainer(SchemaTab(self.reader))
            with TabPane("Data", id="tab-data"):
                yield ScrollableContainer(DataTab(self.reader))
            with TabPane("Metadata", id="tab-metadata"):
                yield ScrollableContainer(MetadataTab(self.reader))
            with TabPane("Stats", id="tab-stats"):
                yield ScrollableContainer(StatsTab(self.reader))
        yield Footer()
