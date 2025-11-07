"""Terminal UI for exploring Parquet files."""

from textual.app import App, ComposeResult
from textual.containers import Container, VerticalScroll
from textual.widgets import Footer, Header, Static

from datanomy.reader import ParquetReader


class DatanomyApp(App):
    """A Textual app to explore Parquet file anatomy."""

    CSS = """
    #file-info {
        padding: 1;
        background: $panel;
        border: solid $primary;
    }

    #schema {
        padding: 1;
        background: $panel;
        border: solid $primary;
        margin-top: 1;
    }

    #row-groups {
        padding: 1;
        background: $panel;
        border: solid $primary;
        margin-top: 1;
    }

    .section-title {
        text-style: bold;
        color: $accent;
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
        yield VerticalScroll(
            Container(
                Static(self._render_file_info(), id="file-info"),
                Static(self._render_schema_arrow(), id="schema"),
                Static(self._render_row_groups(), id="row-groups"),
            )
        )
        yield Footer()

    def _render_file_info(self) -> str:
        """
        Render file information.

        Returns
        -------
            str: Formatted file information
        """
        file_size_mb = self.reader.file_size / (1024 * 1024)
        return (
            "[bold]File Information[/bold]\n\n"
            f"File: {self.reader.file_path.name}\n"
            f"Size: {file_size_mb:.2f} MB\n"
            f"Rows: {self.reader.num_rows:,}\n"
            f"Row Groups: {self.reader.num_row_groups}"
        )

    def _render_schema_arrow(self) -> str:
        """
        Render Arrow schema information.

        Returns
        -------
            str: Formatted Arrow schema information
        """
        schema = self.reader.schema_arrow
        lines = ["[bold]Arrow Schema[/bold]\n"]

        for field in schema:
            lines.append(f"  • {field.name}: {field.type}")

        return "\n".join(lines)

    def _render_row_groups(self) -> str:
        """
        Render row group information.

        Returns
        -------
            str: Formatted row group information
        """
        lines = ["[bold]Row Groups[/bold]\n"]

        for i in range(self.reader.num_row_groups):
            rg = self.reader.get_row_group_info(i)
            size_mb = rg.total_byte_size / (1024 * 1024)
            lines.append(f"  Row Group {i}: {rg.num_rows:,} rows, {size_mb:.2f} MB")

        return "\n".join(lines)
