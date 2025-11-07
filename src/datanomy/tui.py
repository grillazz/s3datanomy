"""Terminal UI for exploring Parquet files."""

from rich.console import Group
from rich.panel import Panel
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
        Format size in bytes to human-readable format.

        Parameters
        ----------
            size_bytes: Size in bytes

        Returns
        -------
            str: Formatted size string (e.g., "1.23 KB", "45.67 MB")
        """
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.2f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.2f} MB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"

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
        header_panel = Panel(
            Text("Magic Number: PAR1", style="yellow"),
            title="Header",
            border_style="yellow",
        )

        # Row groups
        row_group_panels = []
        for i in range(self.reader.num_row_groups):
            rg = self.reader.get_row_group_info(i)
            rg_size_str = self._format_size(rg.total_byte_size)

            rg_text = Text()
            rg_text.append(f"Rows: {rg.num_rows:,}\n")
            rg_text.append(f"Size: {rg_size_str}\n")
            rg_text.append(f"Columns: {rg.num_columns}")

            panel = Panel(
                rg_text, title=f"[green]Row Group {i}[/green]", border_style="green"
            )
            row_group_panels.append(panel)

        # Footer metadata
        metadata_size_str = self._format_size(self.reader.metadata_size)
        footer_text = Text()
        footer_text.append(f"Total Rows: {self.reader.num_rows:,}\n")
        footer_text.append(f"Row Groups: {self.reader.num_row_groups}\n")
        footer_text.append(f"Metadata Size: {metadata_size_str}\n\n")
        footer_text.append("Magic Number: PAR1", style="yellow")

        footer_panel = Panel(
            footer_text, title="[blue]Footer (Metadata)[/blue]", border_style="blue"
        )

        # Combine all sections
        sections: list[Text | Panel] = [file_info, Text(), header_panel, Text()]
        sections.extend(row_group_panels)
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
