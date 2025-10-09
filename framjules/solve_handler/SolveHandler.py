import gc
from pathlib import Path
from time import time

from framcore import Base, Model
from framcore.attributes import StockVolume
from framcore.components import Component, Flow, Node
from framcore.expressions import get_level_value, is_convertable
from framcore.querydbs import CacheDB, ModelDB, QueryDB
from framcore.timeindexes import AverageYearRange, FixedFrequencyTimeIndex
from framcore.utils import get_one_commodity_storage_subsystems, get_supported_components

from framjules import JulESConfig
from framjules.solve_handler.build_handler.BuildHandler import BuildHandler
from framjules.solve_handler.build_handler.SerialBuildHandler import SerialBuildHandler
from framjules.solve_handler.config_handler.ConfigHandler import ConfigHandler
from framjules.solve_handler.config_handler.SerialConfigHandler import SerialConfigHandler
from framjules.solve_handler.dataclasses import ComponentInfo, DomainModels, GraphInfos, NodeFlowGraphs
from framjules.solve_handler.JulESAggregator import JulESAggregator
from framjules.solve_handler.JulESNames import JulESNames
from framjules.solve_handler.results_handler.SerialResultsHandler import SerialResultsHandler
from framjules.solve_handler.run_handler.SerialRunHandler import SerialRunHandler


class SolveHandler(Base):
    """Common data methods for different simulation modes."""

    def __init__(self, folder: Path, clearing_model: Model, config: JulESConfig) -> None:
        """Hold all data and methods needed to solve JulES."""
        names = JulESNames()

        t = time()
        short_aggregations = config.get_short_term_aggregations()  # list of Aggregators
        self.send_debug_event(f"get_short_term_aggregations time: {round(time() - t, 2)} seconds")
        mid_aggregations = []  # config.get_mid_term_aggregations()  # not supported yet
        long_aggregations = []  # config.get_long_term_aggregations()  # not supported yet

        t = time()
        aggregator = JulESAggregator(
            clearing=clearing_model,
            short=short_aggregations,
            medium=mid_aggregations,
            long=long_aggregations,
        )
        self.send_debug_event(f"JulESAggregator init time: {round(time() - t, 2)} seconds")

        t = time()
        short_term_model = aggregator.get_short_term_model()
        self.send_debug_event(f"get_short_term_model init time: {round(time() - t, 2)} seconds")
        t = time()
        medium_term_model = aggregator.get_medium_term_model()
        self.send_debug_event(f"get_medium_term_model init time: {round(time() - t, 2)} seconds")
        t = time()
        long_term_model = aggregator.get_long_term_model()
        self.send_debug_event(f"get_long_term_model init time: {round(time() - t, 2)} seconds")

        t = time()
        domain_models = DomainModels(
            clearing=clearing_model,
            short_term=short_term_model,
            medium_term=medium_term_model,
            long_term=long_term_model,
        )
        self.send_debug_event(f"DomainModels init time: {round(time() - t, 2)} seconds")

        t = time()
        supported_types = (Flow, Node)
        forbidden_types = tuple()
        graphs = NodeFlowGraphs(
            clearing=get_supported_components(
                self._get_components(clearing_model),
                supported_types,
                forbidden_types,
            ),
            short_term=get_supported_components(
                self._get_components(short_term_model),
                supported_types,
                forbidden_types,
            ),
            medium_term=get_supported_components(
                self._get_components(medium_term_model),
                supported_types,
                forbidden_types,
            ),
            long_term=get_supported_components(
                self._get_components(long_term_model),
                supported_types,
                forbidden_types,
            ),
        )
        self.send_debug_event(f"NodeFlowGraphs init time: {round(time() - t, 2)} seconds")

        t = time()
        graph_infos = GraphInfos(
            clearing={k: ComponentInfo() for k in graphs.clearing},
            short_term={k: ComponentInfo() for k in graphs.short_term},
            medium_term={k: ComponentInfo() for k in graphs.medium_term},
            long_term={k: ComponentInfo() for k in graphs.long_term},
        )
        self.send_debug_event(f"GraphInfos init time: {round(time() - t, 2)} seconds")

        # we check that that aggregated models don't have different storages
        t = time()
        aggregator.assert_equal_storages(
            graphs.short_term,
            graphs.medium_term,
            graphs.long_term,
        )
        self.send_debug_event(f"assert_equal_storages time: {round(time() - t, 2)} seconds")

        t = time()
        constructor = CacheDB if config.is_cache_db() else ModelDB
        db = constructor(
            domain_models.clearing,
            domain_models.short_term,
            domain_models.medium_term,
            domain_models.long_term,
        )
        self.send_debug_event(f"DB init time: {round(time() - t, 2)} seconds")

        t = time()
        self.fill_graph_infos(graph_infos, graphs, names, aggregator, config, db)
        self.send_debug_event(f"fill_graph_infos time: {round(time() - t, 2)} seconds")

        # Finally, we set the member data
        self.folder: Path = folder
        self.config: JulESConfig = config
        self.names: JulESNames = names
        self.domain_models: DomainModels = domain_models
        self.graphs: NodeFlowGraphs = graphs
        self.graph_infos: GraphInfos = graph_infos
        # NB! will be freed after self.configure()
        # so we don't hold up memory during run
        self.db: QueryDB = db

    def build(self) -> None:
        """Build input files for JulES."""
        handler = self.create_build_handler()
        handler.build()

    def configure(self) -> None:
        """Build configuration file for JulES."""
        handler = self.create_config_handler()
        handler.configure()

        self.db = None
        gc.collect()

    def run(self) -> None:
        """Run Julia-JulES."""
        handler = self.create_run_handler()
        handler.run()

    def set_results(self) -> None:
        """Set results from Julia-JulES run into domain models."""
        handler = self.create_results_handler()
        handler.set_results()

    def create_build_handler(self) -> BuildHandler:
        """Create specialized BuildHandler for the chosen simulation mode."""
        if self.config.is_simulation_mode_serial():
            handler_constructor = SerialBuildHandler
        else:
            raise NotImplementedError

        return handler_constructor(
            folder=self.folder,
            config=self.config,
            names=self.names,
            domain_models=self.domain_models,
            graphs=self.graphs,
            graph_infos=self.graph_infos,
            db=self.db,
        )

    def create_config_handler(self) -> ConfigHandler:
        """Create specialized ConfigHandler for the chosen simulation mode."""
        if self.config.is_simulation_mode_serial():
            handler_constructor = SerialConfigHandler
        else:
            raise NotImplementedError

        return handler_constructor(
            folder=self.folder,
            config=self.config,
            names=self.names,
            graph_infos=self.graph_infos,
        )

    def create_run_handler(self) -> SerialRunHandler:
        """Create specialized RunHandler for the chosen simulation mode."""
        dependencies = []

        tulipa_version = self.config.get_tulipa_version()
        if tulipa_version is not None:
            if Path.exists(Path(tulipa_version)):
                dependencies.append(tulipa_version)
            else:
                dependencies.append(("https://github.com/NVE/TuLiPa.git", tulipa_version))

        jules_version = self.config.get_jules_version()
        if jules_version is not None:
            if Path.exists(Path(jules_version)):
                dependencies.append(jules_version)
            else:
                dependencies.append(("https://github.com/NVE/JulES.git", jules_version))

        dependencies.extend(["YAML", "HDF5", "JSON", "PythonCall"])

        if self.config.is_simulation_mode_serial():
            handler_constructor = SerialRunHandler
        else:
            message = "JulES Parallel simulation mode is not yet supported."
            raise NotImplementedError(message)
        handler_constructor.ENV_NAME = self.names.JULIA_ENV_NAME
        return handler_constructor(folder=self.folder, config=self.config, names=self.names, dependencies=dependencies)

    def create_results_handler(self) -> SerialResultsHandler:
        """Create a SerialResultsHandler."""
        if self.config.is_simulation_mode_serial():
            handler_constructor = SerialResultsHandler
        else:
            message = "JulES Parallel simulation mode is not yet supported."
            raise NotImplementedError(message)
        return handler_constructor(
            folder=self.folder,
            config=self.config,
            names=self.names,
            graphs=self.graphs,
            graph_infos=self.graph_infos,
        )

    def _get_components(self, model: Model) -> dict[str, Component]:
        return {k: v for k, v in model.get_data().items() if isinstance(v, Component)}

    def fill_graph_infos(
        self,
        graph_infos: GraphInfos,
        graphs: NodeFlowGraphs,
        names: JulESNames,
        aggregator: JulESAggregator,
        config: JulESConfig,
        db: QueryDB,
    ) -> None:
        """Fill graph_info with derived info."""
        # Intent is to gather complex derivations in just one place

        # NB! Order of below method calls matter
        t = time()
        self.set_basic_node_flow_info(graph_infos.clearing, graphs.clearing)
        self.set_basic_node_flow_info(graph_infos.short_term, graphs.short_term)
        self.set_basic_node_flow_info(graph_infos.medium_term, graphs.medium_term)
        self.set_basic_node_flow_info(graph_infos.long_term, graphs.long_term)
        self.send_debug_event(f"set_basic_node_flow_info time: {round(time() - t, 2)} seconds")

        t = time()
        self.set_sss_info(graph_infos.clearing, graphs.clearing, names, config)
        self.set_sss_info(graph_infos.short_term, graphs.short_term, names, config)
        self.set_sss_info(graph_infos.medium_term, graphs.medium_term, names, config)
        self.set_sss_info(graph_infos.long_term, graphs.long_term, names, config)
        self.send_debug_event(f"set_sss_info time: {round(time() - t, 2)} seconds")

        t = time()
        self.set_market_info(graph_infos.clearing, graphs.clearing, names)
        self.set_market_info(graph_infos.short_term, graphs.short_term, names)
        self.set_market_info(graph_infos.medium_term, graphs.medium_term, names)
        self.set_market_info(graph_infos.long_term, graphs.long_term, names)
        self.send_debug_event(f"set_market_info time: {round(time() - t, 2)} seconds")

        t = time()
        self.set_agg_storage_node_info(graph_infos.clearing, aggregator, graphs.clearing, graphs.short_term)
        # self.set_agg_market_node_info(graph_infos.clearing, aggregator, graphs.clearing, graphs.medium_term)
        self.send_debug_event(f"set_agg_storage_node_info time: {round(time() - t, 2)} seconds")

        t = time()
        self.set_jules_id_info(graph_infos.clearing, is_aggregated=False, names=names)
        self.set_jules_id_info(graph_infos.short_term, is_aggregated=True, names=names)
        self.send_debug_event(f"set_jules_id_info time: {round(time() - t, 2)} seconds")

        t = time()
        self.set_unit_info(graph_infos.clearing, graphs.clearing, config, names)
        self.set_unit_info(graph_infos.short_term, graphs.short_term, config, names)
        self.send_debug_event(f"set_unit_info time: {round(time() - t, 2)} seconds")

        t = time()
        self.set_sss_global_eneq_info(graph_infos.clearing, graphs.clearing, db, config)
        self.set_sss_global_eneq_info(graph_infos.short_term, graphs.short_term, db, config)
        self.send_debug_event(f"set_sss_global_eneq_info time: {round(time() - t, 2)} seconds")

        t = time()
        self.set_sss_initial_storage(graph_infos.clearing, graphs.clearing, db, config)
        self.set_agg_initial_storage(graph_infos.short_term, graph_infos.clearing)
        self.send_debug_event(f"set_sss_initial_storage time: {round(time() - t, 2)} seconds")

        t = time()
        # assert that graph_infos has expected content
        assert all({True, False} == {x.is_node, x.is_flow} for x in graph_infos.clearing.values())
        assert all({True, False} == {x.is_node, x.is_flow} for x in graph_infos.short_term.values())
        assert all({True, False} == {x.is_node, x.is_flow} for x in graph_infos.medium_term.values())
        assert all({True, False} == {x.is_node, x.is_flow} for x in graph_infos.long_term.values())
        # TODO: add more checks
        self.send_debug_event(f"validation time: {round(time() - t, 2)} seconds")

    def set_basic_node_flow_info(
        self,
        out_graph_info: dict[str, ComponentInfo],
        graph: dict[str, Flow | Node],
    ) -> None:
        """Info directly accessible from Node and Flow API.

        We also set domain_commodity for Flow as main_node.get_commodity().
        """
        for component_id, c in graph.items():
            info = out_graph_info[component_id]

            info.is_flow = isinstance(c, Flow)
            info.is_node = isinstance(c, Node)

            info.is_exogenous = c.is_exogenous()

            info.is_storage_node = isinstance(c, Node) and c.get_storage() is not None

            if info.is_node:
                info.main_node_id = component_id
                info.domain_commodity = c.get_commodity()

            if info.is_flow:
                info.main_node_id = c.get_main_node()
                info.domain_commodity = graph[info.main_node_id].get_commodity()
                info.num_arrows = len(c.get_arrows())

    def set_sss_info(
        self,
        out_graph_info: dict[str, ComponentInfo],
        graph: dict[str, Flow | Node],
        names: JulESNames,
        config: JulESConfig,
    ) -> None:
        """Storage SubSystem (sss) info."""
        include_boundaries = False  # so market nodes at the boundary is not incorrectly classified
        subsystems = get_one_commodity_storage_subsystems(graph, include_boundaries)

        for info in out_graph_info.values():
            info.has_storage_resolution = False

        for subsystem_id, (__, subsystem, boundary_domain_commodities) in subsystems.items():
            is_short_term = self.is_short_term_storage_subsystem(subsystem, graph, config)

            jules_commodity = names.SHORT_TERM_STORAGE if is_short_term else names.STORAGE_SYSTEM

            if len(boundary_domain_commodities) == 0:
                message = (
                    f"Warning! No boundary domain commodity found for storage subsystem {subsystem_id} "
                    f"with members {subsystem}.\n"
                )
                print(message)
                for component_id in subsystem:
                    info = out_graph_info[component_id]
                    info.jules_commodity = jules_commodity
                continue
            assert len(boundary_domain_commodities) == 1
            market_commodity = next(iter(boundary_domain_commodities))

            for component_id in subsystem:
                info = out_graph_info[component_id]

                info.is_sss_member = True

                info.sss_id = subsystem_id
                info.sss_is_short_term = is_short_term
                info.sss_market_commodity = market_commodity
                info.sss_members = subsystem

                info.jules_commodity = jules_commodity

            # all nodes in subsystem get True below since include_boundaries = False
            # Flow get False if any arrow points to market commodity
            assert include_boundaries is False
            for component_id in subsystem:
                component = graph[component_id]
                info = out_graph_info[component_id]
                info.has_storage_resolution = True
                if isinstance(component, Flow):
                    for arrow in component.get_arrows():
                        node_id = arrow.get_node()
                        node_info = out_graph_info[node_id]
                        if node_info.jules_commodity == info.sss_market_commodity:
                            info.has_storage_resolution = False
                            break

    def set_market_info(
        self,
        out_graph_info: dict[str, ComponentInfo],
        graph: dict[str, Flow | Node],
        names: JulESNames,
    ) -> None:
        """Set is_market_node and if so, also set jules_commodity to market."""
        for component_id, info in out_graph_info.items():
            info.is_market_node = info.is_node and not info.is_storage_node and not info.is_sss_member

            if info.is_market_node:
                info.jules_commodity = names.MARKET

        for component_id, info in out_graph_info.items():
            info.is_market_flow = False
            if info.is_flow:
                flow = graph[component_id]
                for arrow in flow.get_arrows():
                    if out_graph_info[arrow.get_node()].is_market_node:
                        info.is_market_flow = True
                        break

    def set_agg_storage_node_info(
        self,
        out: dict[str, ComponentInfo],
        aggregator: JulESAggregator,
        detailed_graph: dict[str, Flow | Node],
        aggregated_graph: dict[str, Flow | Node],
    ) -> None:
        """Aggregate storages and update info.agg_storage_node_id."""
        agg_storage_node_ids = {
            n: c for n, c in aggregated_graph.items() if isinstance(c, Node) and c.get_storage() is not None
        }
        graph_map = aggregator.get_short_term_graph_map(detailed_graph, agg_storage_node_ids)
        for member_id, agg_node_ids in graph_map.items():
            assert len(agg_node_ids) > 0
            assert sum(int(n in agg_storage_node_ids) for n in agg_node_ids) == 1
            info = out[member_id]
            if info.is_storage_node:
                for agg_node_id in agg_node_ids:
                    if agg_node_id in agg_storage_node_ids:
                        info.agg_storage_node_id = agg_node_id
                        break

    def set_agg_market_node_info(
        self,
        out: dict[str, ComponentInfo],
        aggregator: JulESAggregator,
        detailed_graph: dict[str, Flow | Node],
        aggregated_graph: dict[str, Flow | Node],
    ) -> None:
        """Aggregate market nodes and update info.agg_market_node_id."""
        market_nodes = {n: c for n, c in aggregated_graph.items() if out[n].is_market_node}
        graph_map = aggregator.get_medium_term_graph_map(detailed_graph, market_nodes)
        for agg_market_node_id, member_node_ids in graph_map.items():
            agg_component = aggregated_graph[agg_market_node_id]
            if not isinstance(agg_component, Node):
                continue
            if agg_component.get_storage() is not None:
                continue
            for node_id in member_node_ids:
                info = out[node_id]
                if info.is_market_node:
                    info.agg_market_node_id = agg_market_node_id

    def set_jules_id_info(
        self,
        out: dict[str, ComponentInfo],
        is_aggregated: bool,
        names: JulESNames,
    ) -> None:
        """Add jules ids in compliance with required format."""
        # Warning! Julia-JulES currently requires this format
        # TODO: revise after removal of hard coding in Julia-JulES
        for node_id, info in out.items():
            info.jules_global_eneq_id = f"{names.GLOBALENEQ}_{node_id}"

            if info.is_storage_node:
                if is_aggregated:
                    info.jules_balance_id = f"{info.jules_commodity}Balance_{node_id}_hydro_reservoir"
                    info.jules_storage_id = f"Reservoir_{node_id}_hydro_reservoir"
                else:
                    info.jules_balance_id = f"{info.jules_commodity}Balance_{node_id}"
                    info.jules_storage_id = f"Reservoir_{node_id}"

            elif info.is_node:
                info.jules_balance_id = f"{info.jules_commodity}Balance_{node_id}"

    def set_unit_info(
        self,
        out: dict[str, ComponentInfo],
        graph: dict[str, Flow | Node],
        config: JulESConfig,
        names: JulESNames,
    ) -> None:
        """Calculate all types of target units.

        Need from config:
        - unit_money
        - unit_stock per commodity for each storage_node
        - unit_flow per commodity for each flow

        Will derive:
        - unit_price per commodity for each market_node
        - unit_cost for each flow
        - unit_coeffs for each flow
        - unit_eneq for each sss_member in each sss

        And also for each flow, we derive:
        - unit_param_type
        - unit_param_flow_unit
        - unit_param_flow_unit

        """
        unit_money = config.get_currency()

        node_info = {k: info for k, info in out.items() if info.is_node}
        flow_info = {k: info for k, info in out.items() if info.is_flow}
        market_node_info = {k: info for k, info in node_info.items() if info.is_market_node}
        storage_node_info = {k: info for k, info in node_info.items() if info.is_storage_node}
        sss_member_info = {k: info for k, info in node_info.items() if info.is_sss_member}

        for info in market_node_info.values():
            unit_stock = config.get_unit_stock(info.domain_commodity)
            info.unit_price = f"{unit_money}/{unit_stock}"

        for info in storage_node_info.values():
            unit_flow = config.get_unit_flow(out[info.main_node_id].domain_commodity)
            unit_stock = config.get_unit_stock(out[info.main_node_id].domain_commodity)
            info.unit_flow = unit_flow
            info.unit_stock = unit_stock

        for d in [flow_info, storage_node_info]:
            for info in d.values():
                unit_flow = config.get_unit_flow(out[info.main_node_id].domain_commodity)
                unit_stock = config.get_unit_stock(out[info.main_node_id].domain_commodity)
                info.unit_flow = unit_flow
                info.unit_stock = unit_stock
                info.unit_cost = f"{unit_money}/{unit_stock}"
                if is_convertable(info.unit_flow, "MW"):
                    info.unit_param_type = names.MWTOGWHPARAM
                    info.unit_param_unit_flow = "MW"
                    info.unit_param_unit_stock = "GWh"
                elif is_convertable(info.unit_flow, "m3/s"):
                    info.unit_param_type = names.M3STOMM3PARAM
                    info.unit_param_unit_flow = "m3/s"
                    info.unit_param_unit_stock = "Mm3"
                else:
                    message = f"Unsupported unit_flow: {info.unit_flow}"
                    raise ValueError(message)

                if info.is_market_flow:
                    seconds = config.get_time_resolution().get_clearing_market_minutes() * 60
                else:
                    seconds = config.get_time_resolution().get_clearing_storage_minutes() * 60
                info.unit_flow_result = f"{unit_stock}/({seconds} * s)"

        for flow_id, info in flow_info.items():
            flow: Flow = graph[flow_id]
            info.unit_coeffs = dict()
            for arrow in flow.get_arrows():
                from_node_id = arrow.get_node()
                unit_coeff = None
                if from_node_id != info.main_node_id:
                    from_node_unit = config.get_unit_stock(out[from_node_id].domain_commodity)
                    unit_coeff = None if from_node_unit == info.unit_stock else f"{from_node_unit}/{info.unit_stock}"
                info.unit_coeffs[from_node_id] = unit_coeff

        for info in sss_member_info.values():
            if info.sss_global_eneq_unit is not None:
                continue
            unit_market = config.get_unit_stock(info.sss_market_commodity)
            unit_stock = config.get_unit_stock(info.domain_commodity)
            unit_eneq = f"{unit_market}/{unit_stock}"
            for component_id in info.sss_members:
                member_info = out[component_id]
                member_info.sss_global_eneq_unit = unit_eneq

    def set_sss_global_eneq_info(
        self,
        out: dict[str, ComponentInfo],
        graph: dict[str, Flow | Node],
        db: QueryDB,
        config: JulESConfig,
    ) -> dict[str, float]:
        """Set global_energy_coefficient using metadata. Convert to usable unit."""
        for component_id, info in out.items():
            # TODO: is_storage_node check is edge case due to module in dataset that can be removed (mod_11_96401_LINVASSELV)
            if not info.is_sss_member or not info.is_storage_node:
                continue

            data_dim: FixedFrequencyTimeIndex = config.get_data_period()

            start_year, num_years = config.get_weather_years()
            scen_dim = AverageYearRange(start_year, num_years)

            metakeys = graph[component_id].get_meta_keys()
            if "EnergyEqDownstream" in metakeys:  # TODO: Use same enekv_global everywhere
                metadata = graph[component_id].get_meta("EnergyEqDownstream")
            elif "enekv_global" in metakeys:
                metadata = graph[component_id].get_meta("enekv_global")
            else:
                message = (
                    f"Missing metadata EnergyEqDownstream or enekv_global for {component_id}, "
                    f"only metadata keys {list(metakeys)}."
                )
                message = message + f" Object info: {info}"
                raise ValueError(message)
            expr = metadata.get_value()

            info.sss_global_eneq_value = get_level_value(
                expr=expr,
                unit=info.sss_global_eneq_unit,
                db=db,
                data_dim=data_dim,
                scen_dim=scen_dim,
                is_max=False,
            )

    def set_sss_initial_storage(
        self,
        out: dict[str, ComponentInfo],
        graph: dict[str, Flow | Node],
        db: QueryDB,
        config: JulESConfig,
    ) -> dict[str, float]:
        """Set sss_initial_storage. Convert to usable unit."""
        for node_id, info in out.items():
            if not info.is_storage_node:
                continue

            node: Node = graph[node_id]

            # TODO: Remove except fallback after adding Node.get_initial_storage_percentage
            percentage = 0.6
            try:
                percentage = node.get_initial_storage_percentage()
                assert 0 <= percentage <= 1
            except Exception:
                self.send_warning_event(
                    f"Missing initial storage for {node_id}. Using 60 % of capacity.",
                )

            info.sss_initial_storage = self._get_initial_storage_capacity(
                node_id,
                node,
                info,
                percentage,
                db,
                config,
            )

    def _get_initial_storage_capacity(
        self,
        node_id: str,
        node: Node,
        info: ComponentInfo,
        percentage: float,
        db: QueryDB,
        config: JulESConfig,
    ) -> float:
        data_dim: FixedFrequencyTimeIndex = config.get_data_period()

        if data_dim.get_num_periods() > 1:
            # TODO: Assert is_simulation_mode_parallel:  Use first period
            raise NotImplementedError

        start_year, num_years = config.get_weather_years()
        scen_dim = AverageYearRange(start_year, num_years)

        capacity: StockVolume = node.get_storage().get_capacity()
        data_value: float = capacity.get_data_value(
            db=db,
            level_period=data_dim,
            scenario_horizon=scen_dim,
            unit=info.unit_stock,
            is_max_level=True,
        )

        return data_value * percentage

    def set_agg_initial_storage(
        self,
        agg_graph_info: dict[str, ComponentInfo],
        det_graph_info: dict[str, ComponentInfo],
    ) -> None:
        """Set global_eneq and initial_storage in aggregated graph_info from detailed graph_info."""
        for det_id, det in det_graph_info.items():
            if not det.is_storage_node:
                continue
            if det.agg_storage_node_id is None:
                continue

            agg = agg_graph_info[det.agg_storage_node_id]

            if agg.sss_initial_storage is None:
                agg.sss_initial_storage = 0.0

            agg.sss_initial_storage += det.sss_global_eneq_value * det.sss_initial_storage

    def is_short_term_storage_subsystem(self, subsystem: set[str], graph: NodeFlowGraphs, config: JulESConfig) -> bool:
        """Return True if is_short_term_storage_system."""
        return False
