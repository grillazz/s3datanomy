def format_size(size_bytes: int) -> str:
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
