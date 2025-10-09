"""Handling of results produced by running JulES in Serial mode."""

from pathlib import Path

from framcore.components import Flow, Node
from framcore.expressions import Expr, ensure_expr
from framcore.timevectors import ConstantTimeVector, LinearTransformTimeVector, LoadedTimeVector, ReferencePeriod

from framjules.JulESConfig import JulESConfig
from framjules.loaders.time_vector_loaders import (
    DemandJulESH5TimeVectorLoader,
    JulESH5TimeVectorLoader,
    SupplyJulESH5TimeVectorLoader,
)
from framjules.solve_handler.dataclasses import ComponentInfo, GraphInfos, NodeFlowGraphs
from framjules.solve_handler.JulESNames import JulESNames

# Must handle decomposition of results since this is serial simulation,
# so the prices are stored along the scenario dimension. To comply with
# the LevelProfile-system, we need to extract the mean price level,
# and normalize the price vector with it, to make the profile into a
# mean-one profile.

# We must also handle that flow units in the output are stored using
# different standards in different parts of the output.


class SerialResultsHandler:
    """Set serial simulation results."""

    def __init__(
        self,
        folder: Path | str,
        config: JulESConfig,
        names: JulESNames,
        graphs: NodeFlowGraphs,
        graph_infos: GraphInfos,
    ) -> None:
        """Handle retrieval of results from a JulES Serial simulation.

        Args:
            folder (Path | str): Path to folder of the JulES simulation.
            config (JulESConfig): Simulation config.
            names (JulESNames): JulES namespace.
            graphs (NodeFlowGraphs): Graphs used in the simulation.
            graph_infos (GraphInfos): JulES specific info of each Component in the graphs.

        """
        self._folder = Path(folder)
        self._config = config
        self._names = names

        self.graphs = graphs
        self.graph_infos = graph_infos

        self.units = self._set_units()

    def set_results(self) -> None:
        """Set JulES results of all Components in the clearing graph."""
        is_whole_years = self._config.is_simulation_mode_serial()

        loader = JulESH5TimeVectorLoader(
            source=self._folder,
            units=self.units,
            relative_loc=self._names.FILENAME_H5_OUTPUT,
            is_whole_years=is_whole_years,
        )

        supply_loader = SupplyJulESH5TimeVectorLoader(
            source=self._folder,
            units=self.units,
            relative_loc=self._names.FILENAME_H5_OUTPUT,
            is_whole_years=is_whole_years,
        )

        demand_loader = DemandJulESH5TimeVectorLoader(
            source=self._folder,
            units=self.units,
            relative_loc=self._names.FILENAME_H5_OUTPUT,
            is_whole_years=is_whole_years,
        )

        power_nodes = [
            name
            for name, c in self.graphs.clearing.items()
            if isinstance(c, Node) and c.get_commodity() == JulESNames.POWER
        ]
        for name, c in self.graphs.clearing.items():
            info: ComponentInfo = self.graph_infos.clearing[name]

            if isinstance(c, Node):
                if c.is_exogenous():
                    continue  # TODO: Fixed reservoirs
                self._set_node_results(c, name, loader)
            if isinstance(c, Flow):
                self._set_flow_results(
                    c,
                    info,
                    name,
                    loader,
                    power_nodes,
                    supply_loader=supply_loader,
                    demand_loader=demand_loader,
                )

    def _set_node_results(self, node: Node, name: str, loader: JulESH5TimeVectorLoader) -> None:
        info = self.graph_infos.clearing[name]

        if info.is_market_node:
            level, profile = self._get_decomposed_level_profile(name, loader, self.units[name])
            price = node.get_price()
            price.set_level(level)
            price.set_profile(profile)

        if info.is_storage_node:
            level, profile = self._get_decomposed_level_profile(
                info.jules_storage_id,
                loader,
                self.units[info.jules_storage_id],
                is_stock=True,
            )
            storage = node.get_storage()
            volume = storage.get_volume()
            volume.set_level(level)
            volume.set_profile(profile)

            level, profile = self._get_decomposed_level_profile(
                info.jules_storage_id + "_sv", loader, self.units[info.jules_storage_id + "_sv"]
            )
            price = node.get_price()
            price.set_level(level)
            price.set_profile(profile)

    def _set_flow_results(
        self,
        flow: Flow,
        info: ComponentInfo,
        name: str,
        loader: JulESH5TimeVectorLoader,
        power_nodes: list[str],
        supply_loader: SupplyJulESH5TimeVectorLoader,
        demand_loader: DemandJulESH5TimeVectorLoader,
    ) -> None:
        if info.is_exogenous and flow.get_volume().get_level():
            return
        if info.is_exogenous:
            max_capacity = flow.get_max_capacity()
            level = max_capacity.get_level()
            profile = max_capacity.get_profile()
        else:
            level, profile = self._get_decomposed_level_profile(
                name,
                loader,
                self.units[name],
                is_flow=True,
            )
        volume = flow.get_volume()
        volume.set_level(level)
        volume.set_profile(profile)

        # TODO: improve h5 file in Julia-JulES to avoid this workaround
        n = len(name)
        for arrow, volume in flow.get_arrow_volumes().items():
            arrow_node = arrow.get_node()
            if arrow_node not in power_nodes:
                continue

            is_supply = arrow.is_ingoing()
            loader = supply_loader if is_supply else demand_loader

            unit_flow = self.units[name]
            unit_coeff = info.unit_coeffs[arrow.get_node()]
            unit = f"({unit_flow})*({unit_coeff})" if unit_coeff is not None else unit_flow

            for i in range(n):
                subname = name[: n - i]
                level, profile = self._get_decomposed_level_profile(
                    subname,
                    loader,
                    unit,
                    is_flow=True,
                )
                volume.set_level(level)
                volume.set_profile(profile)
                break

    def _get_decomposed_level_profile(
        self,
        name: str,
        loader: JulESH5TimeVectorLoader,
        unit: str | None = None,
        is_flow: bool = False,
        is_stock: bool = False,
    ) -> tuple[Expr, Expr]:
        """Decompose price vector into level and profile expressions."""
        # TODO: handle negative prices

        timevector = self._get_timevector(jules_id=name, loader=loader)

        # TODO: Clear cashe due to this? Do we need loader.clear_cache()
        mean_value = timevector.get_vector(self._config.is_float32()).mean()

        scale = float(1 / mean_value) if mean_value != 0 else 1.0
        mean_one_profile_timevector = LinearTransformTimeVector(
            timevector=timevector,
            scale=scale,
            shift=0.0,
            unit=None,
            is_zero_one_profile=False,
        )

        profile_expr = None
        reference_period = None
        if mean_value != 0:
            reference_period = self._get_reference_period()
            profile_expr = ensure_expr(mean_one_profile_timevector, is_profile=True)

        avg_level_timevector = ConstantTimeVector(
            scalar=mean_value,
            unit=unit,
            is_max_level=False,
            reference_period=reference_period,
        )

        level_expr = ensure_expr(
            avg_level_timevector,
            is_level=True,
            is_flow=is_flow,
            is_stock=is_stock,
            profile=profile_expr,
        )

        return level_expr, profile_expr

    def _set_units(self) -> dict[str : str | None]:
        units = {name: info.unit_price for name, info in self.graph_infos.clearing.items() if info.is_market_node}
        units.update(
            {
                info.jules_storage_id: info.unit_stock
                for info in self.graph_infos.clearing.values()
                if info.is_storage_node
            },
        )
        units.update(
            {
                info.jules_storage_id + "_sv": info.unit_cost
                for info in self.graph_infos.clearing.values()
                if info.is_storage_node and info.unit_cost is not None
            },
        )
        units.update(
            {name: info.unit_flow_result for name, info in self.graph_infos.clearing.items() if info.is_flow},
        )
        return units

    def _get_reference_period(self) -> ReferencePeriod:
        first_year, num_years = self._config.get_simulation_years()
        return ReferencePeriod(first_year, num_years)

    def _get_timevector(self, jules_id: str, loader: JulESH5TimeVectorLoader) -> LoadedTimeVector:
        try:
            return LoadedTimeVector(vector_id=jules_id, loader=loader)
        except Exception:
            raise AssertionError from None
