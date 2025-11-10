from rich.table import Table


def create_column_grid(num_columns: int) -> Table:
    """
    Create a standard column grid for displaying panels.

    Parameters
    ----------
        num_columns: Number of columns in the grid

    Returns
    -------
        Table: Configured grid with equal-ratio columns
    """
    grid = Table.grid(padding=(0, 1), expand=True)
    for _ in range(num_columns):
        grid.add_column(ratio=1, min_width=20)
    return grid
