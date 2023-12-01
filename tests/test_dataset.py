import datetime
import tempfile

import pyarrow as pa

from datascape.dataset import Dataset


def test_init_dataset():
    """Test Dataset initialization."""
    with tempfile.TemporaryDirectory() as tmpdir:
        dataset = Dataset(tmpdir)
        assert dataset.location == tmpdir


def test_filename_template():
    """Test Dataset initialization."""
    nanos = 1701433027125334597
    timestamp = datetime.datetime.fromtimestamp(nanos // 1_000_000_000)
    remaining_nanos = nanos % 1_000_000_000
    expected = f"part-{timestamp.strftime('%Y%m%dT%H%M%S')}.{remaining_nanos}-{{i}}.parquet"

    with tempfile.TemporaryDirectory() as tmpdir:
        dataset = Dataset(tmpdir)
        assert dataset._get_filename_template(nanos_epoch=nanos) == expected


def test_append_records_to_empty():
    """Test Dataset initialization."""
    schema = pa.schema([("part", pa.string()), ("name", pa.string()), ("value", pa.int64())])

    records = [
        dict(part="a", name="Foo", value=1),
        dict(part="b", name="Bar", value=2),
        dict(part="a", name="Baz", value=3)]

    with tempfile.TemporaryDirectory() as tmpdir:
        dataset = Dataset(tmpdir, partition_on=["part"], schema=schema)
        dataset.append_records(records)
        assert dataset.count_rows() == len(records)
