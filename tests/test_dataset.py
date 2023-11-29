import tempfile

import pytest

from datascape.dataset import Dataset


def test_init_dataset():
    """Test Dataset initialization."""
    with tempfile.TemporaryDirectory() as tmpdir:
        dataset = Dataset(tmpdir)
        assert dataset.location == tmpdir


@pytest.mark.skip(reason="Not yet implemented")
def test_populate_dataset_with_records():
    """Test Dataset initialization."""

    records = [
        dict(part="a", name="Foo", value=1),
        dict(part="b", name="Bar", value=2),
        dict(part="a", name="Baz", value=3)]

    with tempfile.TemporaryDirectory() as tmpdir:
        dataset = Dataset(tmpdir)
        dataset.add_records(records)
