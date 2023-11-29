"""Dataset tools.
"""

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
                 partition_on: List[Tuple[str, Any] | str] = None
                 ) -> None:
        """Initialize a Dataset object.

        Arguments:
            location: The location of the dataset.
            partition_on: (optional). List of field names to partition on.
        """
        self.location = location

        if partition_on:
            part_schema = pa.schema([(p, pa.string()) if isinstance(p, str) else p for p in partition_on])
            self.dataset = ds.dataset(
                location,
                partitining=ds.partitioning(part_schema, flavor="hive"))
        else:
            self.dataset = ds.dataset(location)

    def add_records(self, records: Iterable[Any]) -> None:
        raise NotImplementedError("Not yet implemented")
        ds.write_dataset(records, schema=self.dataset.schema)
