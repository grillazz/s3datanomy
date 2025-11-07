"""Basic tests for datanomy."""

import datanomy


def test_version() -> None:
    """Test that version is defined."""
    assert hasattr(datanomy, "__version__")
    assert isinstance(datanomy.__version__, str)
