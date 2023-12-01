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
                 partition_flavor: str = "hive",
                 schema: pa.Schema = None
                 ) -> None:
        """Initialize a Dataset object.

        Arguments:
            location: The location of the dataset.
            partition_on: (optional). List of field names to partition on.
        """
        self.location = location
        self.schema = schema
        self.partitioning = ds.partitioning(
            self._fields_to_schema(partition_on),
            flavor=partition_flavor) if partition_on else None
        self._init_dataset()

    def _init_dataset(self) -> None:
        self.dataset = ds.dataset(
            self.location,
            schema=self.schema,
            partitioning=self.partitioning
        )

    def _fields_to_schema(self, partition_on: List[Tuple[str, Any] | str]) -> pa.Schema:
        return pa.schema([(p, pa.string()) if isinstance(p, str) else p for p in partition_on])

    def _get_filename_template(self, nanos_epoch: int = None) -> str:
        if nanos_epoch is None:
            nanos = time.time_ns()
        else:
            nanos = nanos_epoch

        timestamp = datetime.datetime.fromtimestamp(nanos // 1_000_000_000)
        remaining_nanos = nanos % 1_000_000_000
        return f"part-{timestamp.strftime('%Y%m%dT%H%M%S')}.{remaining_nanos}-{{i}}.parquet"

    def append_records(self, records: Iterable[Any]) -> None:
        ds.write_dataset(
            pa.RecordBatch.from_pylist(records),
            self.location,
            schema=self.dataset.schema,
            format=self.dataset.format,
            partitioning=self.partitioning,
            basename_template=self._get_filename_template())
        self._init_dataset()

    def count_rows(self) -> int:
        return self.dataset.count_rows()
