"""Basic package-level tests for datanomy."""

import datanomy


def test_version() -> None:
    """Test that version is defined and valid."""
    assert hasattr(datanomy, "__version__")
    assert isinstance(datanomy.__version__, str)
    assert len(datanomy.__version__) > 0
    # Should be semver-ish (has dots)
    assert "." in datanomy.__version__
