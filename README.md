# Datanomy

> Explore the anatomy of your columnar data files

**Datanomy** is a terminal-based tool for inspecting and understanding data files.
It provides an interactive view of your data's structure, metadata, and internal organization.

![Parquet demo](https://github.com/user-attachments/assets/87f2f0ad-2eec-480d-b461-370ac3af6122)

## Features (MVP)

- **Schema inspection**: View column names and types
- **Row group details**: See how your data is organized
- **File metadata**: Size, row counts, and more
- **Rich terminal UI**: Navigate with ease using Textual

## Installation

```bash
# From PyPI (coming soon)
pip install datanomy

# From source
git clone https://github.com/raulcd/datanomy.git
cd datanomy
uv sync
```

## Usage

```bash
# Inspect a Parquet file
datanomy data.parquet
```

## Keyboard Shortcuts

- `q` - Quit the application

## Development

```bash
# Install dependencies
uv sync

# Run from source
uv run datanomy path/to/file.parquet
```

```bash
# Install dev dependencies
uv sync --extra dev

# Run tests
uv run pytest

# Format code
uv run ruff format .

# Lint
uv run ruff check .

# Lint
uv run mypy .
```

## Roadmap

- [ ] Data preview
- [ ] Column statistics
- [ ] Arrow IPC file support
- [ ] Compression details
- [ ] Dictionary encoding info
- [ ] Export reports

## License

Apache License 2.0

## Contributing

Contributions welcome! Please open an issue or PR.

---

Built with [Textual](https://textual.textualize.io/) and [PyArrow](https://arrow.apache.org/docs/python/)
