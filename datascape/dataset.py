"""Dataset tools.
"""
import datetime
import time

from typing import Any, Iterable, List, Tuple

import pyarrow as pa
import pyarrow.dataset as ds


class Dataset:
    """A dataset.

    Attributes:
        dataset (ds.Dataset): The underlying dataset.
    """

    def __init__(self,
                 location,
                 *,
                 partition_on: List[Tuple[str, Any] | str] = None,
                 partition_flavor: str = "hive"
                 ) -> None:
        """Initialize a Dataset object.

        Arguments:
            location: The location of the dataset.
            partition_on: (optional). List of field names to partition on.
        """
        self.location = location
        self.partitioning = ds.partitioning(
            self._fields_to_schema(partition_on),
            flavor=partition_flavor) if partition_on else None
        self.dataset = ds.dataset(location, partitioning=self.partitioning)

    def _fields_to_schema(self, partition_on: List[Tuple[str, Any] | str]) -> pa.Schema:
        return pa.schema([(p, pa.string()) if isinstance(p, str) else p for p in partition_on])

    def _get_filename_template(self):
        nanos = time.time_ns()
        timestamp = datetime.datetime.fromtimestamp(nanos // 1_000_000_000)
        remaining_nanos = nanos % 1_000_000_000
        return f"part-{timestamp.strftime('%Y%m%dT%H%M%S')}.{remaining_nanos}-{{i}}.parquet"

    def add_records(self, records: Iterable[Any]) -> None:
        raise NotImplementedError("Not yet implemented")
        ds.write_dataset(records, schema=self.dataset.schema)
