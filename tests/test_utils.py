"""Basic package-level tests for datanomy."""

import datanomy
from datanomy.utils import format_size


def test_version() -> None:
    """Test that version is defined and valid."""
    assert hasattr(datanomy, "__version__")
    assert isinstance(datanomy.__version__, str)
    assert len(datanomy.__version__) > 0
    # Should be semver-ish (has dots)
    assert "." in datanomy.__version__


def test_format_size() -> None:
    """Test the format_size utility function."""
    assert format_size(500) == "500 bytes"
    assert format_size(2048) == "2.00 KB (2,048 bytes)"
    assert format_size(5 * 1024 * 1024) == "5.00 MB (5,242,880 bytes)"
    assert format_size(3 * 1024 * 1024 * 1024) == "3.00 GB (3,221,225,472 bytes)"
