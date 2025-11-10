"""Terminal UI for exploring Parquet files."""

from textual.app import App, ComposeResult
from textual.containers import ScrollableContainer
from textual.widgets import Footer, Header, TabbedContent, TabPane

from datanomy.reader.parquet import ParquetReader
from datanomy.tui.parquet import DataTab, MetadataTab, SchemaTab, StatsTab, StructureTab


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
