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

    def _render_structure(self) -> Group:
        """
        Render the Parquet file structure diagram.

        Returns
        -------
            Group: Rich renderable showing file structure
        """
        file_size_mb = self.reader.file_size / (1024 * 1024)

        # File info panel
        file_info = Text()
        file_info.append("File: ", style="bold")
        file_info.append(f"{self.reader.file_path.name}\n")
        file_info.append("Size: ", style="bold")
        file_info.append(f"{file_size_mb:.2f} MB")

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
            rg_size_mb = rg.total_byte_size / (1024 * 1024)

            rg_text = Text()
            rg_text.append(f"Rows: {rg.num_rows:,}\n")
            rg_text.append(f"Size: {rg_size_mb:.2f} MB\n")
            rg_text.append(f"Columns: {rg.num_columns}")

            panel = Panel(
                rg_text, title=f"[green]Row Group {i}[/green]", border_style="green"
            )
            row_group_panels.append(panel)

        # Footer metadata
        footer_text = Text()
        footer_text.append(f"Total Rows: {self.reader.num_rows:,}\n")
        footer_text.append(f"Row Groups: {self.reader.num_row_groups}\n\n")
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

    def _render_schema(self) -> str:
        """
        Render schema information.

        Returns
        -------
            str: Formatted schema information
        """
        schema = self.reader.schema_arrow
        lines = ["[bold cyan]Schema[/bold cyan]\n"]

        for i, field in enumerate(schema, 1):
            lines.append(f"{i:3d}. [green]{field.name}[/green]: {field.type}")

        return "\n".join(lines)


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
