import json
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import timedelta
from pathlib import Path
from time import time

import numpy as np
from framcore import Base
from framcore.attributes import Arrow, Cost, FlowVolume, Price, StockVolume, Storage
from framcore.components import Flow, Node
from framcore.expressions import get_units_from_expr
from framcore.querydbs import QueryDB
from framcore.timeindexes import FixedFrequencyTimeIndex, ProfileTimeIndex
from numpy.typing import NDArray

from framjules import JulESConfig
from framjules.solve_handler.build_handler.DataElementAppender import DataElementAppender
from framjules.solve_handler.dataclasses import ComponentInfo, DomainModels, GraphInfos, NodeFlowGraphs
from framjules.solve_handler.JulESNames import JulESNames


class BuildHandler(Base, ABC):
    """Responsible for implementing shared functionality in build method."""

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
        """Initialize handler.

        Use inputs passed down from SolveHandler

        and create extra fields only relevant for
        the build phase.
        """
        self.folder = folder
        self.config = config
        self.names = names
        self.domain_models = domain_models
        self.graphs = graphs
        self.graph_infos = graph_infos
        self.db = db

        self.append = DataElementAppender(names)

        self.errors: set[str] = set()
        self.timevectors: dict[FixedFrequencyTimeIndex, dict[str, NDArray]] = defaultdict(dict)

    def build(self) -> None:
        """Build input and configuration files for JulES."""
        t = time()
        self.build_data_elements(self.names.CLEARING, self.graphs.clearing, self.graph_infos.clearing)
        self.send_debug_event(f"build_data_elements clearing time: {round(time() - t, 2)} seconds")

        t = time()
        self.build_data_elements(self.names.AGGREGATED, self.graphs.short_term, self.graph_infos.short_term)
        self.send_debug_event(f"build_data_elements aggregated time: {round(time() - t, 2)} seconds")

        # Intent is to build data elements for aggregated models. Pending changes in Julia-JulES
        # self.build_data_elements(self.names.SHORT_TERM, self.graphs.short_term, self.graph_infos.short_term)
        # self.build_data_elements(self.names.MEDIUM_TERM, self.graphs.medium_term, self.graph_infos.medium_term)
        # self.build_data_elements(self.names.LONG_TERM, self.graphs.long_term, self.graph_infos.long_term)

        t = time()
        self.build_time_vectors()
        self.send_debug_event(f"build_time_vectors time: {round(time() - t, 2)} seconds")

        t = time()
        self.build_storage_mapping(self.graph_infos.clearing)
        self.send_debug_event(f"build_storage_mapping time: {round(time() - t, 2)} seconds")

        t = time()
        self.build_start_storage(self.names.FILENAME_START_STORAGES_CLEARING, self.graph_infos.clearing)
        self.send_debug_event(f"build_start_storage clearing time: {round(time() - t, 2)} seconds")

        t = time()
        self.build_start_storage(self.names.FILENAME_START_STORAGES_AGGREGATED, self.graph_infos.short_term)
        self.send_debug_event(f"build_start_storage aggregated time: {round(time() - t, 2)} seconds")

    def build_start_storage(
        self,
        filename: str,
        graph_info: dict[str, ComponentInfo],
    ) -> None:
        """Write start storag json file to folder."""
        data = {
            info.jules_storage_id: info.sss_initial_storage for k, info in graph_info.items() if info.is_storage_node
        }
        self.write_json_file(data, filename)

    def build_data_elements(
        self,
        model_id: str,
        graph: dict[str, Flow | Node],
        graph_info: dict[str, ComponentInfo],
    ) -> None:
        """Write json file with data elements for a graph belonging to a given model_id."""
        self.fill_data_elements(model_id, graph, graph_info)
        self.stop_if_errors()
        filename = f"{self.names.ROOT_FILENAME_DATAELEMENTS}_{model_id}.json"
        self.write_json_file(self.append.data_elements, filename)

    def stop_if_errors(self) -> None:
        """Throw RunTimeError if any errors."""
        if self.errors:
            error_string = "\n".join(self.errors)
            message = f"Errors found:\n{error_string}"
            raise RuntimeError(message)

    def build_time_vectors(self) -> None:
        """Write json file with time vector data elements and csv file for each unique time index."""
        self.append.data_elements = []
        for i, timeindex in enumerate(self.timevectors, start=1):
            time_index_id = f"timeindex_{i}"

            milliseconds: float = timeindex.get_period_duration().total_seconds() * 1000.0
            time_delta_id = f"{time_index_id}_timedelta"
            self.append.ms_time_delta(time_delta_id, milliseconds)

            self.append.range_time_index(
                time_index_id,
                timeindex.get_start_time(),
                timeindex.get_num_periods(),
                time_delta_id,
            )

            table_id = f"{time_index_id}_table"
            path_table, column_names = self.write_table(time_index_id, timeindex)
            self.append.base_table(table_id, path_table, column_names)

            is_one_year = timeindex.is_one_year()

            for column_name in column_names:
                time_vector_id = column_name
                time_values_id = f"{time_vector_id}_values"

                self.append.column_time_values(time_values_id, table_id, column_name)

                if is_one_year:
                    self.append.one_year_time_vector(time_vector_id, time_index_id, time_values_id)
                else:
                    self.append.rotating_time_vector(time_vector_id, time_index_id, time_values_id)

        self.write_json_file(self.append.data_elements, self.names.FILENAME_DATAELEMENTS_TIMEVECTORS)

    def get_time_index_id(self, timeindex: FixedFrequencyTimeIndex) -> str:
        """Return id that works in file name."""
        type_name = type(timeindex).__name__
        num_periods = timeindex.get_num_periods()
        resolution = int(timeindex.get_period_duration().total_seconds() * 1000.0)
        is_52 = timeindex.is_52_week_years()
        extr_first = timeindex.extrapolate_first_point()
        extr_last = timeindex.extrapolate_last_point()
        return f"timeindex_{type_name}_periods_{num_periods}_ms_{resolution}_{is_52}_{extr_first}_{extr_last}"

    def write_table(
        self,
        time_index_id: str,
        timeindex: FixedFrequencyTimeIndex,
    ) -> tuple[Path, list[str]]:
        """Write all vectors corresponding to time_index_id to a csv file."""
        vectors = self.timevectors[timeindex]
        column_names = list(vectors.keys())
        matrix = np.column_stack([vectors[c] for c in column_names])
        matrix = np.round(matrix, decimals=6)  # quick fix for negative values in time vectors
        # if negative values give warning
        if np.any(matrix < 0):
            self.errors.add(f"Negative values found in time vector for {time_index_id}. This might cause issues.")
        filename = f"timevector_{time_index_id}.csv"
        path = self.folder / filename
        np.savetxt(path, matrix, delimiter=",")
        return path, column_names

    def build_storage_mapping(
        self,
        graph_info: dict[str, ComponentInfo],
    ) -> None:
        """Write the mapping of storages from Clearing to Aggregated Model to json."""
        data = {
            k: info.agg_storage_node_id
            for k, info in graph_info.items()
            if info.is_storage_node and info.agg_storage_node_id
        }
        self.write_json_file(data, self.names.FILENAME_STORAGE_MAPPING)

    def fill_data_elements(
        self,
        model_id: str,
        graph: dict[str, Flow | Node],
        graph_info: dict[str, ComponentInfo],
    ) -> None:
        """Reset and fill self.append.data_elements with data element json data."""
        self.append.data_elements = []  # Important to reset the list

        nodes: dict[str, Node] = {k: v for k, v in graph.items() if isinstance(v, Node)}
        flows: dict[str, Flow] = {k: v for k, v in graph.items() if isinstance(v, Flow)}

        exogenous_nodes = {k: v for k, v in nodes.items() if v.is_exogenous()}
        exogenous_flows = {k: v for k, v in flows.items() if v.is_exogenous()}
        endogenous_nodes = {k: v for k, v in nodes.items() if not v.is_exogenous()}
        endogenous_flows = {k: v for k, v in flows.items() if not v.is_exogenous()}

        t = time()
        self.add_exogenous_nodes(exogenous_nodes, graph_info)
        self.send_debug_event(f"add_exogenous_nodes time: {round(time() - t, 2)} seconds")
        t = time()
        self.add_exogenous_flows(exogenous_flows, graph_info)
        self.send_debug_event(f"add_exogenous_flows time: {round(time() - t, 2)} seconds")
        t = time()
        self.add_endogenous_nodes(endogenous_nodes, graph_info, model_id)
        self.send_debug_event(f"add_endogenous_nodes time: {round(time() - t, 2)} seconds")
        t = time()
        self.add_endogenous_flows(endogenous_flows, graph_info)
        self.send_debug_event(f"add_endogenous_flows time: {round(time() - t, 2)} seconds")

        t = time()
        self.add_dummy_exogenous_balance()
        self.send_debug_event(f"add_dummy_exogenous_balance time: {round(time() - t, 2)} seconds")

    def add_dummy_exogenous_balance(self) -> None:
        """Add a dummy exogenous Node for JulES."""
        balance_id = "PowerBalance_DummyNode"
        profile_id = f"{balance_id}_Profile"
        # Find longest name.
        longest_name = ""
        for name in self.graphs.clearing:
            if len(name) > len(longest_name):
                longest_name = name
        if len(longest_name) >= len(balance_id):
            unique_postfix = "_" + "x" * (len(longest_name) - len(balance_id))  # Fill to guarantee uniqueness.
            balance_id += unique_postfix
            profile_id += unique_postfix

        # Set balance and profile ids in names so other parts of system have access to them.
        self.names.dummy_exogenous_balance_name = balance_id
        self.names.dummy_exogenous_profile_id = profile_id

        price_param_id = f"{balance_id}_price_param"
        self.append.exogenous_balance(balance_id, self.names.MARKET, price_param_id)
        self.append.mean_series_param(price_param_id, 1.0, profile_id)

        # Set index and vector so they are added to the dataset in build_time_vectors.

        first_scenario_year, num_scenario_years = self.config.get_weather_years()

        period_duration = timedelta(minutes=self.config.get_time_resolution().get_clearing_market_minutes())

        dummy_timeindex = ProfileTimeIndex(
            start_year=first_scenario_year,
            num_years=num_scenario_years,
            period_duration=self._get_closest_valid_profile_duration(period_duration),
            is_52_week_years=True,
        )

        if dummy_timeindex not in self.timevectors:
            default_vector = np.arange(0, dummy_timeindex.get_num_periods(), 1, dtype=np.float64)
            np.divide(default_vector, default_vector.max(), out=default_vector)
            # TODO: Replace default_vector with derived vector (e.g. based on rhs) or user supplied vector
            self.timevectors[dummy_timeindex] = {profile_id: default_vector}

    def add_exogenous_nodes(
        self,
        exogenous_nodes: dict[str, Node],
        graph_info: dict[str, ComponentInfo],
    ) -> None:
        """Append exogenous balance related data elements for exogenous node."""
        for node_id, node in exogenous_nodes.items():
            info = graph_info[node_id]

            balance_id = info.jules_balance_id

            price_param_id = f"{balance_id}_price_param"

            self.append.exogenous_balance(balance_id, info.jules_commodity, price_param_id)

            price: Price = node.get_price()

            if not price.has_level():
                message = f"Node {node_id} is exogenous but has not price."
                raise RuntimeError(message)

            # TODO: move to get_attribute_profile. applies to all profiles
            if price.has_profile():
                units = get_units_from_expr(self.db, price.get_profile())
                if units:
                    message = f"Node {node_id} has exogenous price profile with units {units}."
                    raise RuntimeError(message)

            level = self.get_price_level(price_param_id, price, info)
            profile = self.get_price_profile(price_param_id, price, info)

            self.append.mean_series_param(price_param_id, level, profile)

    def add_endogenous_nodes(
        self,
        endogenous_nodes: dict[str, Node],
        graph_info: dict[str, ComponentInfo],
        model_id: str,
    ) -> None:
        """Append endogenous balance related data elements for endogenous node."""
        for node_id, node in endogenous_nodes.items():
            info = graph_info[node_id]

            self.append.endogenous_balance(info.jules_balance_id, info.jules_commodity)

            storage = node.get_storage()
            if storage is not None:
                self.add_storage(model_id, storage, info)

    def add_exogenous_flows(
        self,
        exogenous_flows: dict[str, Flow],
        graph_info: dict[str, ComponentInfo],
    ) -> None:
        """Append data elements related to an exogenous flow."""
        for flow_id, flow in exogenous_flows.items():
            for arrow in flow.get_arrows():
                node_info = graph_info[arrow.get_node()]
                if not node_info.is_exogenous:
                    self.add_rhs_term(flow_id, flow, arrow, node_info, graph_info[flow_id])

    def add_endogenous_flows(
        self,
        endogenous_flows: dict[str, Flow],
        graph_info: dict[str, ComponentInfo],
    ) -> None:
        """Append data elements for endogenous flows and related attributes."""
        for flow_id, flow in endogenous_flows.items():
            self.append.base_flow(flow_id)

            flow_info = graph_info[flow_id]

            self.add_flow_lower_bound(flow_id, flow, flow_info)
            self.add_flow_upper_bound(flow_id, flow, flow_info)
            self.add_flow_arrows(flow_id, flow, graph_info)
            self.add_flow_costs(flow_id, flow, flow_info)

    def add_storage(
        self,
        model_id: str,
        storage: Storage,
        info: ComponentInfo,
    ) -> None:
        """Append data elements related to a storage."""
        # TODO: Support soft bounds, storage loss
        storage_id = info.jules_storage_id
        balance_id = info.jules_balance_id

        self.append.base_storage(storage_id, balance_id)

        capacity = storage.get_capacity()

        self.add_positive_capacity(storage_id, info, capacity, f"{storage_id}_upper_bound", False)

        self.append.lower_zero_capacity(f"{storage_id}_lower_bound", info.is_flow, storage_id)

        if model_id == self.names.CLEARING:
            self.append.global_eneq(info.jules_global_eneq_id, info.jules_balance_id, info.sss_global_eneq_value)

    def add_rhs_term(
        self,
        flow_id: str,
        flow: Flow,
        arrow: Arrow,
        node_info: ComponentInfo,
        flow_info: ComponentInfo,
    ) -> None:
        """Append data elements related to rhs term."""
        node_id = arrow.get_node()

        rhs_term_id = f"exogenous_flow_{flow_id}_{node_id}"
        unit_param_id = f"{rhs_term_id}_unit_param"
        series_param_id = f"{rhs_term_id}_series_param"
        balance_id = node_info.jules_balance_id

        self.append.base_rhs_term(rhs_term_id, balance_id, arrow.is_ingoing(), unit_param_id)

        level = self.get_rhs_term_level(rhs_term_id, flow, arrow, flow_info)

        profile = self.get_rhs_term_profile(rhs_term_id, flow, arrow, flow_info)

        # unit is actually flipped from main to target node
        # using conversion factor inside get_rhs_term_level
        # so the jules unit param might say GWh but the values
        # have been converted so that the result will become e.g. Mm3

        self.append.unit_param(unit_param_id, series_param_id, flow_info)
        self.append.mean_series_param(series_param_id, level, profile)

    def add_flow_lower_bound(
        self,
        flow_id: str,
        flow: Flow,
        flow_info: ComponentInfo,
    ) -> None:
        """Append lower bound related data elements for a flow."""
        capacity = flow.get_min_capacity()
        bound_id = f"{flow_id}_lower_bound"
        profile = None if capacity is None else capacity.get_profile()
        if profile is None:
            self.append.lower_zero_capacity(bound_id, flow_info.is_flow, flow_or_storage_id=flow_id)
            return
        self.add_positive_capacity(flow_id, flow_info, capacity, bound_id, is_lower_bound=True)

    def add_flow_upper_bound(
        self,
        flow_id: str,
        flow: Flow,
        flow_info: ComponentInfo,
    ) -> None:
        """Append upper bound related data elements for a flow."""
        capacity = flow.get_max_capacity()
        if capacity is None:
            return
        bound_id = f"{flow_id}_upper_bound"
        self.add_positive_capacity(flow_id, flow_info, capacity, bound_id, is_lower_bound=False)

    def add_flow_arrows(
        self,
        flow_id: str,
        flow: Flow,
        graph_info: dict[str, ComponentInfo],
    ) -> None:
        """Append arrow related data elements for each arrow in flow."""
        # TODO: Add support for SegmentedArrow
        flow_info = graph_info[flow_id]
        for arrow in flow.get_arrows():
            assert arrow.has_profile() is False, "Not supported in MVP"  # TODO

            arrow_id = f"{flow_id}_arrow_{arrow.get_node()}->{flow_info.main_node_id}"

            level = self.get_coefficient_level(arrow_id, arrow, flow_info)

            balance_id = graph_info[arrow.get_node()].jules_balance_id

            self.append.base_arrow(arrow_id, flow_id, balance_id, arrow.is_ingoing(), level)

    def add_flow_costs(
        self,
        flow_id: str,
        flow: Flow,
        flow_info: ComponentInfo,
    ) -> None:
        """Append cost data element for each cost in flow."""
        cost_terms = flow.get_cost_terms()
        for cost_term_id, cost_term in cost_terms.items():
            level = self.get_cost_term_level(cost_term_id, cost_term, flow_info)

            has_profile = cost_term.get_profile() is None

            profile = self.get_cost_term_profile(cost_term_id, cost_term, flow_info) if has_profile else 1.0

            extended_cost_term_id = f"{flow_id}_{cost_term_id}"
            param_id = f"{extended_cost_term_id}_param"
            self.append.cost_term(extended_cost_term_id, flow_id, flow_info.is_flow, cost_term.is_cost(), param_id)

            self.append.mean_series_param(param_id, level, profile)

    def add_positive_capacity(
        self,
        flow_or_storage_id: str,
        info: ComponentInfo,
        capacity: FlowVolume | StockVolume,
        bound_id: str,
        is_lower_bound: bool,
    ) -> None:
        """Append data elements related to positive capacity."""
        series_param_id = f"{bound_id}_series_param"

        if isinstance(capacity, FlowVolume):
            unit_param_id = f"{bound_id}_unit_param"
            self.append.unit_param(unit_param_id, series_param_id, info)
            self.append.positive_capacity(bound_id, info.is_flow, flow_or_storage_id, is_lower_bound, unit_param_id)
        else:
            assert isinstance(capacity, StockVolume)
            self.append.positive_capacity(bound_id, info.is_flow, flow_or_storage_id, is_lower_bound, series_param_id)

        level = self.get_capacity_level(series_param_id, capacity, info)
        profile = self.get_capacity_profile(series_param_id, capacity, info)

        self.append.mean_series_param(series_param_id, level, profile)

    def write_json_file(self, data: object, filename: str) -> None:
        """Write data to json."""
        with Path.open(self.folder / filename, "w") as f:
            json.dump(data, f, indent=self.names.JSON_INDENT)

    def _get_closest_valid_profile_duration(self, period_duration: timedelta) -> timedelta:
        input_seconds = period_duration.total_seconds()
        data = [h * 3600 for h in [168, 84, 56, 42, 28, 24, 21, 14, 12, 8, 7, 6, 4, 3, 2, 1]]
        for profile_seconds in data:
            if profile_seconds <= input_seconds:
                break
        return timedelta(seconds=profile_seconds)

    # Must be implemented for each simulation mode

    @abstractmethod
    def get_price_level(self, root_id: str, price: Price, info: ComponentInfo) -> str | float:
        """Query price level."""
        pass

    @abstractmethod
    def get_price_profile(self, root_id: str, price: Price, info: ComponentInfo) -> str | float:
        """Query price profile."""
        pass

    @abstractmethod
    def get_capacity_level(self, root_id: str, capacity: FlowVolume | StockVolume, info: ComponentInfo) -> str | float:
        """Query capacity level."""
        pass

    @abstractmethod
    def get_capacity_profile(
        self,
        root_id: str,
        capacity: FlowVolume | StockVolume,
        info: ComponentInfo,
    ) -> str | float:
        """Query capacity profile."""
        pass

    @abstractmethod
    def get_coefficient_level(self, root_id: str, arrow: Arrow, info: ComponentInfo) -> str | float:
        """Query arrow coefficient level."""
        pass

    @abstractmethod
    def get_cost_term_level(self, root_id: str, cost_term: Cost, info: ComponentInfo) -> str | float:
        """Query cost term level."""
        pass

    @abstractmethod
    def get_cost_term_profile(self, root_id: str, cost_term: Cost, info: ComponentInfo) -> str | float:
        """Query cost term profile."""
        pass

    @abstractmethod
    def get_rhs_term_level(
        self,
        rhs_term_id: str,
        flow: Flow,
        arrow: Arrow,
        flow_info: ComponentInfo,
    ) -> str | float:
        """Query rhs term level."""
        pass

    @abstractmethod
    def get_rhs_term_profile(
        self,
        rhs_term_id: str,
        flow: Flow,
        arrow: Arrow,
        flow_info: ComponentInfo,
    ) -> str | float:
        """Query rhs term profile."""
        pass
