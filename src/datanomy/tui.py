"""Terminal UI for exploring Parquet files."""

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
            rg_size_str = self._format_size(rg.total_byte_size)

            # Summary info
            rg_summary = Text()
            rg_summary.append(f"Rows: {rg.num_rows:,}\n")
            rg_summary.append(f"Size: {rg_size_str}\n")
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
                row_panels = []
                for col_offset in range(cols_per_row):
                    col_idx = row_idx + col_offset
                    if col_idx < cols_to_display:
                        col = rg.column(col_idx)
                        col_size_str = self._format_size(col.total_compressed_size)
                        col_name = col.path_in_schema

                        col_text = Text()
                        col_text.append(f"Size: {col_size_str}\n", style="dim")
                        col_text.append(f"Type: {col.physical_type}", style="dim")

                        col_panel = Panel(
                            col_text,
                            title=f"[cyan]{col_name}[/cyan]",
                            border_style="dim",
                            padding=(0, 1),
                        )
                        row_panels.append(col_panel)
                    else:
                        # Empty panel for alignment
                        row_panels.append(Panel("", border_style="dim", padding=(0, 1)))

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
        page_index_panels = []
        if page_index_size > 0:
            page_index_size_str = self._format_size(page_index_size)

            # Column Index panel
            col_index_text = Text()
            col_index_text.append("Per-page statistics for filtering\n", style="dim")
            col_index_text.append(f"Combined size: {page_index_size_str}", style="dim")
            col_index_panel = Panel(
                col_index_text,
                title="[magenta]Column Index[/magenta]",
                border_style="magenta",
            )
            page_index_panels.append(col_index_panel)

            # Offset Index panel
            offset_index_text = Text()
            offset_index_text.append("Page locations for random access\n", style="dim")
            offset_index_text.append("(included in combined size above)", style="dim")
            offset_index_panel = Panel(
                offset_index_text,
                title="[magenta]Offset Index[/magenta]",
                border_style="magenta",
            )
            page_index_panels.append(offset_index_panel)

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

    def _render_schema(self) -> Group:
        """
        Render schema information.

        Returns
        -------
            Group: Rich renderable showing both Arrow and Parquet schemas
        """
        # Arrow Schema
        arrow_schema = self.reader.schema_arrow
        arrow_lines = []
        for i, field in enumerate(arrow_schema, 1):
            arrow_lines.append(f"{i:3d}. [green]{field.name}[/green]: {field.type}")

        arrow_text = Text("\n".join(arrow_lines))
        arrow_panel = Panel(
            arrow_text, title="[cyan]Arrow Schema[/cyan]", border_style="cyan"
        )

        # Parquet Schema
        parquet_schema_str = str(self.reader.schema_parquet)
        parquet_panel = Panel(
            Text(parquet_schema_str),
            title="[yellow]Parquet Schema[/yellow]",
            border_style="yellow",
        )

        return Group(arrow_panel, Text(), parquet_panel)


class DatanomyApp(App):
    """A Textual app to explore Parquet file anatomy."""

    CSS = """
    TabbedContent {
        height: 1fr;
    }

    TabPane {
        padding: 1;
    }

    #structure-content, #schema-content {
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
                yield Static("[dim]Data preview - Coming soon[/dim]")
            with TabPane("Metadata", id="tab-metadata"):
                yield Static("[dim]File metadata - Coming soon[/dim]")
            with TabPane("Stats", id="tab-stats"):
                yield Static("[dim]Column statistics - Coming soon[/dim]")
        yield Footer()
