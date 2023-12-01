"""Dataset tools.
"""
import datetime
import time

from typing import Any, Iterable, List, Tuple

import pyarrow as pa
import pyarrow.dataset as ds


TYPE_STRING = "string"
TYPE_INT64 = "int64"
TYPE_INT32 = "int32"
TYPE_DOUBLE = "float64"


def to_pyarrow_type(type_name: str) -> pa.DataType:
    """Convert a type name to a pyarrow type."""
    return {
        TYPE_STRING: pa.string(),
        TYPE_INT64: pa.int64(),
        TYPE_INT32: pa.int32(),
        TYPE_DOUBLE: pa.float64()
    }[type_name]


class ArrowDataset:
    """A wrapper around a pyarrow dataset.

    Attributes:
        dataset (ds.Dataset): The underlying dataset.
    """

    def __init__(self,
                 location,
                 *,
                 partition_on: List[Tuple[str, Any] | str] = None,
                 partition_flavor: str = "hive",
                 schema: List[Tuple[str, str]] = None
                 ) -> None:
        """Initialize a Dataset object.

        Arguments:
            location: The location of the dataset.
            partition_on: (optional). List of field names to partition on.
        """
        # TODO if the location doesn't exist yet a schema and partition scheme must be provided
        # TODO if location exists, scheman and partition scheme should be inferred (or provided)
        self.location = location
        self.schema = pa.schema([(f, to_pyarrow_type(t)) for f, t in schema]) if schema else None
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

    def append_records(self, records: List[Any]) -> None:
        ds.write_dataset(
            pa.RecordBatch.from_pylist(records),
            self.location,
            schema=self.dataset.schema,
            format=self.dataset.format,
            partitioning=self.partitioning,
            existing_data_behavior="overwrite_or_ignore",
            basename_template=self._get_filename_template())
        self._init_dataset()

    def count_rows(self) -> int:
        return self.dataset.count_rows()
