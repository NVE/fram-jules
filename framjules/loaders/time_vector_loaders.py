from datetime import datetime
from pathlib import Path
from typing import ClassVar

import h5py
import numpy as np
from framcore.fingerprints.fingerprint import Fingerprint
from framcore.loaders.loaders import FileLoader, TimeVectorLoader
from framcore.timeindexes import FixedFrequencyTimeIndex, ListTimeIndex, TimeIndex
from numpy.typing import NDArray


class JulESH5TimeVectorLoader(FileLoader, TimeVectorLoader):
    """Loader for JulES H5 files containing time vectors."""

    _SUPPORTED_SUFFIXES: ClassVar[list] = [".h5"]
    # find time vector id in one of the names, then use the corresponding names to get the data
    _SEARCH_FIELDS: ClassVar[list] = [
        ("areanames", "priceindex", "pricematrix", True),
        ("batnames", "batindex", "batmatrix", True),
        ("resnames", "resindex", "resmatrix_water", True),
        ("othernames_Vars_Power", "priceindex", "othervalues_Vars_Power", True),
        ("othernames_Vars_Hydro", "resindex", "othervalues_Vars_Hydro", True),
        (
            "storagenames",
            "stateindex",
            "storagevalues_main",
            True,
        ),  # TODO: allow collecting different results from the same ids # noqa FIX002
    ]
    _DEFAULT_INDEX = "priceindex"

    def __init__(
        self,
        source: Path | str,
        units: dict[str, str],
        relative_loc: Path | str | None = None,
        is_whole_years: bool = False,
    ) -> None:
        """Initialize the NVEH5TimeVectorLoader."""
        super().__init__(source, relative_loc)
        self._data: dict[str, NDArray] = {}
        self._index: dict[str, TimeIndex] = {}
        self._units: dict[str, str] = units
        self._is_whole_years = is_whole_years

        self._id_fields_map: dict[str, list[set]] = {}

    def clear_cache(self) -> None:
        """Clear cached data."""
        self._data = {}
        self._index = {}
        self._id_fields_map = {}

    def get_reference_period(self, vector_id: str) -> None:
        """Return None."""
        return

    def is_max_level(self, vector_id: str) -> None:
        """Return None."""
        return

    def is_zero_one_profile(self, vector_id: str) -> None:
        """Return None."""
        return True

    def get_values(self, vector_id: str) -> NDArray:
        """Find the values for a given vector id.

        Args:
            vector_id: (str)

        Returns:
            NDArray: Values for the vector id.

        """
        # TODO: Change output format in JuLES to save vectors, and read vectors one at a time. # noqa FIX002
        self._id_exsists(vector_id)
        self._check_multiple_fields(vector_id)
        if vector_id not in self._data:
            id_field, values_field = self._get_id_values_field(vector_id)
            with h5py.File(self.get_source(), "r") as f:
                ids = np.array([item.decode() for item in f[id_field]])
                values_matrix = np.array(f[values_field])
                new_data = {id_: values_matrix[i, :] for i, id_ in enumerate(ids)}
                self._data.update(new_data)
        # index may not use all values
        index = self.get_index(vector_id)
        vector = self._data[vector_id]
        n = index.get_num_periods()
        return vector[:n]

    def get_index(self, vector_id: str) -> TimeIndex:
        """Find the time index for a given vector id.

        Args:
            vector_id: (str)

        Returns:
            TimeIndex

        """
        self._id_exsists(vector_id)  # calls get_ids which calls _get_ids which sets _id_fields_map
        self._check_multiple_fields(vector_id)  # therefore we can use this afterwards
        with h5py.File(self.get_source(), "r") as f:
            index_field = self._id_fields_map.get(vector_id)[0][1]
            fmt = "%Y-%m-%dT%H:%M:%S"
            if self._index is None or (index_field not in self._index):
                t0 = datetime.strptime(f[index_field][0].decode(), fmt)
                t1 = datetime.strptime(f[index_field][1].decode(), fmt)
                index = FixedFrequencyTimeIndex(
                    start_time=t0,
                    period_duration=t1 - t0,
                    num_periods=len(f[index_field]),
                    is_52_week_years=False,
                    extrapolate_first_point=False,
                    extrapolate_last_point=False,
                )
                if not index.is_whole_years() and self._is_whole_years:
                    # start time for each period
                    datetime_list = [datetime.strptime(x.decode(), fmt) for x in f[index_field]]
                    period_duration = datetime_list[1] - datetime_list[0]

                    # add end index since JulES index represents periods
                    datetime_list.append(datetime_list[-1] + period_duration)
                    num_periods = len(datetime_list)

                    # find last index before new iso year
                    last_in_year_ix = num_periods - 1
                    while last_in_year_ix >= 0:
                        last_in_year_ix -= 1
                        this_period = datetime_list[last_in_year_ix]
                        next_period = this_period + period_duration
                        this_year = this_period.isocalendar().year
                        next_year = next_period.isocalendar().year
                        if next_year > this_year:
                            break

                    last_in_year = datetime_list[last_in_year_ix]

                    first_next_year = last_in_year.fromisocalendar(last_in_year.isocalendar().year + 1, 1, 1)

                    if last_in_year + period_duration == first_next_year:
                        index = FixedFrequencyTimeIndex(
                            start_time=datetime_list[0],
                            period_duration=period_duration,
                            num_periods=last_in_year_ix + 1,
                            is_52_week_years=False,
                            extrapolate_first_point=False,
                            extrapolate_last_point=False,
                        )
                    elif last_in_year + period_duration > first_next_year:
                        # TODO: test (only reachable without profiles as ProfileTimeIndex enforces whole years)
                        datetime_list[last_in_year_ix + 1] = first_next_year
                        del datetime_list[last_in_year_ix + 2 :]  # (slice delete does not error when out-of-bounds)
                        index = ListTimeIndex(
                            datetime_list=datetime_list,
                            is_52_week_years=False,
                            extrapolate_first_point=False,
                            extrapolate_last_point=False,
                        )
                    else:
                        n = last_in_year_ix
                        message = (
                            f"Unexpected last_in_year + period_duration < first_next_year.\n"
                            f"vector_id = {vector_id}\n"
                            f"last_in_year = {last_in_year}\n"
                            f"period_duration = {period_duration}\n"
                            f"first_next_year = {first_next_year}\n"
                            f"datetime_list around last_in_year_ix = {datetime_list[n - 10 : n + 10]}"
                        )
                        raise RuntimeError(message)

                self._index[index_field] = index
        return self._index[index_field]

    def get_unit(self, vector_id: str) -> str:
        """Get the unit of the time vector."""
        return self._units[vector_id]

    def get_metadata(self) -> str:
        """Get metadata from the file."""
        return ""

    def _get_id_values_field(self, vector_id: str) -> str:
        search_fields = self._id_fields_map.get(vector_id)
        return search_fields[0][0], search_fields[0][2]

    def _get_ids(self) -> list[str]:
        if not self._id_fields_map:
            self._create_id_fields_map()
        return list(self._id_fields_map.keys())

    def _create_id_fields_map(self) -> dict[str, list[str]]:
        if not self._id_fields_map:
            self._id_fields_map: dict[str, list[str]] = dict()
            with h5py.File(self.get_source(), "r") as f:
                for search_name in self._SEARCH_FIELDS:
                    if search_name[0] in f:
                        new_ids = [item.decode() for item in f[search_name[0]]]
                        for vector_id in new_ids:
                            if vector_id not in self._id_fields_map:
                                self._id_fields_map[vector_id] = [search_name]
                            else:
                                self._id_fields_map[vector_id].append(search_name)

    def _check_multiple_fields(self, vector_id: str) -> None:
        self._create_id_fields_map()
        # check if the vector id is found in multiple fields.
        if len(self._id_fields_map[vector_id]) > 1:
            msg = (
                f"Vector ID {vector_id} found in multiple fields: {self._id_fields_map[vector_id]}. "
                "Could not determine which field to use."
            )
            raise NotImplementedError(msg)

    def get_fingerprint(self) -> Fingerprint:
        """Get the fingerprint of the NVEH5TimeVectorLoader."""
        return None

    def __eq__(self, other: object) -> bool:
        """Check if self and other are equal."""
        if not isinstance(other, type(self)):
            return False
        return self.get_source() == other.get_source() and self._SEARCH_FIELDS == other._SEARCH_FIELDS

    def __hash__(self) -> int:
        """Return hash of NVEH5TimeVectorLoader object."""
        return hash(
            (
                self.get_source(),
                frozenset(self._SEARCH_FIELDS),
            ),
        )


class SupplyJulESH5TimeVectorLoader(JulESH5TimeVectorLoader):
    """Workaround to get supply results and at the same time avoid name conflicts."""

    _SUPPORTED_SUFFIXES: ClassVar[list] = [".h5"]
    _SEARCH_FIELDS: ClassVar[list] = [
        ("supplynames", "priceindex", "supplyvalues", True),
    ]
    _DEFAULT_INDEX = "priceindex"
    pass


class DemandJulESH5TimeVectorLoader(JulESH5TimeVectorLoader):
    """Workaround to get demand results and at the same time avoid name conflicts."""

    _SUPPORTED_SUFFIXES: ClassVar[list] = [".h5"]
    _SEARCH_FIELDS: ClassVar[list] = [
        ("demandnames", "priceindex", "demandvalues", True),
    ]
    _DEFAULT_INDEX = "priceindex"
    pass
