from datetime import timedelta
from pathlib import Path

import numpy as np
from framcore.attributes import Arrow, Cost, FlowVolume, LevelProfile, Price, StockVolume
from framcore.components import Flow
from framcore.querydbs import QueryDB
from framcore.timeindexes import (
    AverageYearRange,
    FixedFrequencyTimeIndex,
    ProfileTimeIndex,
    SinglePeriodTimeIndex,
)
from numpy.typing import NDArray

from framjules import JulESConfig
from framjules.solve_handler.build_handler.BuildHandler import BuildHandler
from framjules.solve_handler.dataclasses import ComponentInfo, DomainModels, GraphInfos, NodeFlowGraphs
from framjules.solve_handler.JulESNames import JulESNames


class SerialBuildHandler(BuildHandler):
    """Specialized methods for serial simulation."""

    _PROFILE_VALUE_WHEN_ZERO = 1.0

    def __init__(
        self,
        folder: Path,
        config: JulESConfig,
        names: JulESNames,
        domain_models: DomainModels,
        graphs: NodeFlowGraphs,
        graph_infos: GraphInfos,
        db: QueryDB,
    ) -> None:
        """See BuildHandler."""
        super().__init__(folder, config, names, domain_models, graphs, graph_infos, db)

        # some validations spesific to serial simulation
        self._data_period = self.config.get_data_period()
        self._check_type(self._data_period, SinglePeriodTimeIndex)

        start_year, num_years = self.config.get_weather_years()
        self._avg_year_range = AverageYearRange(start_year=start_year, num_years=num_years)
        if self._avg_year_range.is_52_week_years():
            message = "Expected AverageYearRange to have is_52_week_years() == False."
            raise ValueError(message)

        self._is_float32 = self.config.is_float32()

    def get_attribute_level(
        self,
        root_id: str,
        attribute: LevelProfile,
        target_unit: str | None,
    ) -> float:
        """Get level value."""
        is_max_level = False
        value = attribute.get_data_value(
            db=self.db,
            unit=target_unit,
            level_period=self._data_period,
            scenario_horizon=self._avg_year_range,
            is_max_level=is_max_level,
        )
        if value < 0:
            self.send_warning_event(f"Attribute {root_id} of type {type(attribute).__name__} returned {value}")
        return value

    def get_attribute_profile(
        self,
        root_id: str,
        attribute: LevelProfile | Arrow,
        default: float,
        unit: str | None,
        info: ComponentInfo,
    ) -> str | float:
        """Add profile vector to timevectors and return profile_id."""
        if not attribute.has_profile():
            return default

        profile_id = f"{root_id}_profile"

        timeindex = self._find_profile_timeindex(attribute, info.has_storage_resolution)

        vector: NDArray = attribute.get_scenario_vector(
            db=self.db,
            level_period=self._data_period,
            scenario_horizon=timeindex,
            is_float32=self._is_float32,
            unit=unit,
        )
        vector = np.round(vector, decimals=6)  # avoid very small numerical noise, e.g negative prices from jules run

        if np.isnan(vector).any():
            message = (
                f"Profile {profile_id} in time index {timeindex} contains NaN values. "
                "This may indicate a problem with the data or the configuration."
            )
            raise ValueError(message)

        if (vector < 0).any():
            message = (
                f"Profile {profile_id} in time index {timeindex} has negative values. "
                "This may indicate a problem with the data or the configuration."
            )
            raise ValueError(message)

        denominator = vector.mean()

        if denominator == 0:
            vector.fill(self._PROFILE_VALUE_WHEN_ZERO)
        else:
            np.multiply(vector, 1 / denominator, out=vector)

        self.timevectors[timeindex][profile_id] = vector

        return profile_id

    def _find_profile_timeindex(
        self,
        attribute: LevelProfile | Arrow,
        has_storage_resolution: bool,
    ) -> ProfileTimeIndex:
        is_52_week_years = False

        start_year, num_years = self.config.get_weather_years()

        if has_storage_resolution:
            period_duration = timedelta(minutes=self.config.get_time_resolution().get_clearing_storage_minutes())
        else:
            period_duration = timedelta(minutes=self.config.get_time_resolution().get_clearing_market_minutes())

        fallback = ProfileTimeIndex(
            start_year,
            num_years,
            period_duration=self._get_closest_valid_profile_duration(period_duration),
            is_52_week_years=is_52_week_years,
        )

        ix_set: set[FixedFrequencyTimeIndex] = attribute.get_profile_timeindex_set(db=self.db)
        if not all(isinstance(ix, FixedFrequencyTimeIndex) for ix in ix_set):
            return fallback

        is_one_year = all(ix.is_one_year() for ix in ix_set)

        candidate = min((ix for ix in ix_set), key=lambda ix: ix.get_period_duration().total_seconds())

        s_fallback = fallback.get_period_duration().total_seconds()
        s_candidate = candidate.get_period_duration().total_seconds()
        best_duration = (candidate if s_fallback < s_candidate else fallback).get_period_duration()

        if is_one_year:
            return ProfileTimeIndex(
                start_year=candidate.get_start_time().isocalendar().year,
                num_years=1,
                period_duration=self._get_closest_valid_profile_duration(best_duration),
                is_52_week_years=is_52_week_years,
            )
        return ProfileTimeIndex(
            start_year=start_year,
            num_years=num_years,
            period_duration=self._get_closest_valid_profile_duration(best_duration),
            is_52_week_years=is_52_week_years,
        )

    def get_price_level(self, root_id: str, price: Price, info: ComponentInfo) -> float:
        """get_price_level for serial simulation."""
        return self.get_attribute_level(root_id, price, info.unit_price)

    def get_cost_term_level(self, root_id: str, cost_term: Cost, info: ComponentInfo) -> float:
        """get_cost_term_level for serial simulation."""
        return self.get_attribute_level(root_id, cost_term, info.unit_cost)

    def get_capacity_level(self, root_id: str, capacity: StockVolume | FlowVolume, info: ComponentInfo) -> str | float:
        """get_capacity_level for serial simulation. Handles stock or flow based on info."""
        return self.get_attribute_level(root_id, capacity, info.unit_flow if info.is_flow else info.unit_stock)

    def get_coefficient_level(self, root_id: str, arrow: Arrow, info: ComponentInfo) -> str | float:
        """get_coefficient_level for serial simulation."""
        return self.get_attribute_level(root_id, arrow, info.unit_coeffs[arrow.get_node()])

    def get_price_profile(self, root_id: str, price: Price, info: ComponentInfo) -> str | float:
        """get_price_profile for serial simulation."""
        return self.get_attribute_profile(root_id, price, 1.0, info.unit_price, info)

    def get_cost_term_profile(self, root_id: str, cost_term: Cost, info: ComponentInfo) -> str | float:
        """get_cost_term_profile for serial simulation."""
        return self.get_attribute_profile(root_id, cost_term, 1.0, info.unit_cost, info)

    def get_capacity_profile(
        self,
        root_id: str,
        capacity: FlowVolume | StockVolume,
        info: ComponentInfo,
    ) -> str | float:
        """get_capacity_profile for serial simulation."""
        unit = info.unit_flow if info.is_flow else info.unit_stock
        return self.get_attribute_profile(root_id, capacity, 1.0, unit, info)

    def get_rhs_term_level(
        self,
        rhs_term_id: str,
        flow: Flow,
        arrow: Arrow,
        flow_info: ComponentInfo,
    ) -> float:
        """Convert volume (main node) to target node volume.

        This may scale the volume using arrow coefficient,
        e.g. due to transportation loss.

        This may also change unit, if target node belongs to
        different commodity than main node, such as for hydropower.
        """
        coeff_value = self.get_coefficient_level(rhs_term_id, arrow, flow_info)

        volume = flow.get_volume()
        if volume.has_level():
            volume_value = self.get_attribute_level(
                root_id=rhs_term_id,
                attribute=volume,
                target_unit=flow_info.unit_flow,
            )
        else:
            max_cap = flow.get_max_capacity()
            min_cap = flow.get_min_capacity()
            if max_cap is not None and max_cap == min_cap:
                volume_value = self.get_attribute_level(
                    root_id=rhs_term_id,
                    attribute=max_cap,
                    target_unit=flow_info.unit_flow,
                )
            else:
                message = f"{rhs_term_id} is not exogenous"
                raise ValueError(message)

        if coeff_value == 0:
            message = (
                f"Got zero coeff_value for {rhs_term_id}.\n"
                f"volume_value = {volume_value}\n"
                f"coeff_value = {coeff_value}\n"
                f"flow.get_main_node() = {flow.get_main_node()}\n"
                f"arrow.get_node() = {arrow.get_node()}\n"
            )
            raise RuntimeError(message)

        return volume_value / coeff_value  # convert from main_unit to target_unit

    def _rank_profile_timeindex(self, ix: ProfileTimeIndex) -> tuple[bool, float]:
        return ix.is_one_year(), ix.get_period_duration().total_seconds()

    def _select_profile_timeindex(self, *candidates: ProfileTimeIndex) -> ProfileTimeIndex:
        """Select the one with not-is_one_year (if any) and finest period duration."""
        return min(candidates, key=self._rank_profile_timeindex)

    def get_rhs_term_profile(
        self,
        rhs_term_id: str,
        flow: Flow,
        arrow: Arrow,
        flow_info: ComponentInfo,
    ) -> str | float:
        """Create profile (possibly) representing volume_profile * coefficient_profile."""
        volume = flow.get_volume()

        if not volume.has_profile():
            volume = flow.get_max_capacity()

        not_volume_profile = volume is None or not volume.has_profile()
        not_arrow_profile = not arrow.has_profile()
        has_volume_profile = not not_volume_profile
        has_arrow_profile = not not_arrow_profile

        if not_volume_profile and not_arrow_profile:
            return 1.0

        if has_volume_profile and not_arrow_profile:
            return self.get_attribute_profile(
                rhs_term_id,
                attribute=volume,
                default=1.0,
                unit=flow_info.unit_flow,
                info=flow_info,
            )

        if not_volume_profile and has_arrow_profile:
            unit = flow_info.unit_coeffs[arrow.get_node()]
            return self.get_attribute_profile(
                rhs_term_id,
                attribute=arrow,
                default=1.0,
                unit=unit,
                info=flow_info,
            )

        # Here we get both profiles (volume and coefficient) and muliply them
        # together and store the resulting product profile in self.timevectors

        profile_id = f"{rhs_term_id}_profile"

        timeindex = self._select_profile_timeindex(
            self._find_profile_timeindex(volume, flow_info.has_storage_resolution),
            self._find_profile_timeindex(arrow, flow_info.has_storage_resolution),
        )

        x: NDArray = volume.get_scenario_vector(
            db=self.db,
            level_period=self._data_period,
            scenario_horizon=timeindex,
            is_float32=self._is_float32,
            unit=flow_info.unit_flow,
        )

        y: NDArray = arrow.get_scenario_vector(
            db=self.db,
            level_period=self._data_period,
            scenario_horizon=timeindex,
            is_float32=self._is_float32,
            unit=flow_info.unit_coeffs[arrow.get_node()],
        )

        x_mean = x.mean()
        y_mean = y.mean()
        if x_mean == 0 or y_mean == 0:
            x.fill(self._PROFILE_VALUE_WHEN_ZERO)
            return x

        np.multiply(x, 1.0 / x_mean, out=x)
        np.multiply(y, 1.0 / y_mean, out=y)

        np.multiply(x, y, out=x)

        self.timevectors[timeindex][profile_id] = x

        return profile_id
