# Rules which the given Aggregators must follow:
# Storages must be the same in all aggregations


from framcore import Base, Model
from framcore.aggregators import Aggregator
from framcore.components import Component, Flow, Node

# TODO: somehow check that storages are the same between short, medium and long term Models.
# Probably need to do this in its own function after the aggregation maps for simpler components are created.


class JulESAggregator(Base):
    def __init__(
        self,
        clearing: Model,
        short: list[Aggregator],
        medium: list[Aggregator],
        long: list[Aggregator],
    ) -> None:
        """Class for defining and calculation aggregated Model instances based on a clearing Model.

        Creates three aggregated Model instances for short-, medium- and long-term simulation in JulES.

        Note:
            - Short term Model is aggregated from a Clearing Model.
            - Medium term Model is aggregated from the Short term Model.
            - Long term Model is aggregated from the medium term Model

        Args:
            clearing (Model): The clearing Model to aggregate from.
            short (list[Aggregator]): List of aggregations to create the short term Model from clearing.
            medium (list[Aggregator]): List of aggregations to create the medium term Model from short
            long (list[Aggregator]): List of aggregations to create the long term Model from medium.

        """
        self._clearing = clearing
        self._short = short
        self._medium = medium
        self._long = long

        self._short_model: Model | None = None
        self._medium_model: Model | None = None
        self._long_model: Model | None = None

    def get_short_term_model(self) -> Model:
        """Apply defined aggregations for short term Model."""
        if self._short_model is None:
            self._short_model = self._aggregate(self._clearing, self._short)
        return self._short_model

    def get_medium_term_model(self) -> Model:
        """Apply defined aggregations for medium term Model."""
        if self._medium_model is None:
            self._medium_model = self._aggregate(self.get_short_term_model(), self._medium)
        return self._medium_model

    def get_long_term_model(self) -> Model:
        """Apply defined aggregations for long term Model."""
        if self._long_model is None:
            self._long_model = self._aggregate(self.get_medium_term_model(), self._long)
        return self._long_model

    def get_short_term_aggregation_map(self) -> dict[str, set[str] | None]:
        """Get the aggregation map of Components from clearing to short term Model."""
        return self._create_aggregation_map(self._clearing, self._short)

    def get_medium_term_aggregation_map(self) -> dict[str, set[str] | None]:
        """Get the aggregation map of Components from clearing to medium term Model."""
        return self._create_aggregation_map(self._clearing, self._short + self._medium)

    def get_long_term_aggregation_map(self) -> dict[str, set[str] | None]:
        """Get the aggregation map of Components from clearing to long term Model."""
        return self._create_aggregation_map(self._clearing, self._short + self._medium + self._long)

    def get_short_term_graph_map(
        self,
        graph_clearing: dict[str, Component],
        graph_short: dict[str, Component],
    ) -> dict[str, set[str] | None]:
        """Get aggregation map for version of short term Model with graph of Flows and Nodes."""
        return self._get_graph_aggregation_map(
            original_agg_map=self.get_short_term_aggregation_map(),
            clearing=self._clearing,
            graph_clearing=graph_clearing,
            aggregated=self.get_short_term_model(),
            graph_aggregated=graph_short,
        )

    def get_medium_term_graph_map(
        self,
        graph_clearing: dict[str, Component],
        graph_medium: dict[str, Component],
    ) -> dict[str, set[str] | None]:
        """Get aggregation map for version of medium term Model with graph of Flows and Nodes."""
        return self._get_graph_aggregation_map(
            original_agg_map=self.get_medium_term_aggregation_map(),
            clearing=self._clearing,
            graph_clearing=graph_clearing,
            aggregated=self.get_medium_term_model(),
            graph_aggregated=graph_medium,
        )

    def get_long_term_graph_map(
        self,
        graph_clearing: dict[str, Component],
        graph_long: dict[str, Component],
    ) -> dict[str, set[str] | None]:
        """Get aggregation map for version of long term Model with graph of Flows and Nodes."""
        return self._get_graph_aggregation_map(
            original_agg_map=self.get_long_term_aggregation_map(),
            clearing=self._clearing,
            graph_clearing=graph_clearing,
            aggregated=self.get_long_term_model(),
            graph_aggregated=graph_long,
        )

    def assert_equal_storages(
        self,
        simpler_short: dict[str, Component],
        simpler_medium: dict[str, Component],
        simpler_long: dict[str, Component],
    ) -> None:
        """Check that all Nodes with Storages are preserved between short, medium and long term Models.

        Args:
            simpler_short (dict[str, Component]): Short term Model Components.
            simpler_medium (dict[str, Component]): Medium term Model Components.
            simpler_long (dict[str, Component]): Long term Model Components.

        Raises:
            ValueError: If the Models have differing Storages.

        """
        short_storages = self._get_storages(simpler_short)
        medium_storages = self._get_storages(simpler_medium)
        long_storages = self._get_storages(simpler_long)

        if short_storages != medium_storages != long_storages:
            message = "Storages are not equal between short, medium and long term Models."
            unique_short = short_storages - (medium_storages | long_storages)
            unique_medium = medium_storages - (short_storages | long_storages)
            unique_long = long_storages - (short_storages | medium_storages)
            if unique_short:
                message += f"\n - Unique Nodes with Storages in Short Model: {unique_short}"
            if unique_medium:
                message += f"\n - Unique Nodes with Storages in Medium Model: {unique_medium}"
            if unique_long:
                message += f"\n - Unique Nodes with Storages in Long Model: {unique_long}"
            raise ValueError(message)

    def _aggregate(self, model: Model, aggs: list[Aggregator]) -> Model:
        if aggs:
            # works because aggregators should not modify the original components
            # except if disaggregate is called, but we shall only use aggregate
            agg_model = Model()
            agg_model.get_data().update(model.get_data())
        else:
            agg_model = model
        for agg in aggs:
            agg.aggregate(agg_model)
        return agg_model

    def _create_aggregation_map(self, clearing: Model, aggs: list[Aggregator]) -> dict[str, list[str]]:
        """Merge aggregation maps of a list of aggregators from clearing to final."""
        clearing_ids = [name for name, ob in clearing.get_data().items() if isinstance(ob, Component)]
        full_agg_mapping = {k: {v} for k, v in zip(clearing_ids, clearing_ids, strict=True)}

        for agg in aggs:
            agg_map = agg.get_aggregation_map()
            for detailed_id, aggregated_ids in full_agg_mapping.items():
                if not aggregated_ids:  # Component has been deleted by an aggregation.
                    continue

                new_agg_ids = set()
                for agg_id in aggregated_ids:
                    if agg_id not in agg_map:
                        new_agg_ids.add(agg_id)  # left as is
                        continue
                    if not agg_map[agg_id]:
                        # deleted. if all agg_ids are marked deleted, so is the detailed one.
                        continue
                    new_agg_ids |= agg_map[agg_id]

                full_agg_mapping[detailed_id] = new_agg_ids  # empty set signifies deleted component

        return full_agg_mapping

    def _get_graph_aggregation_map(
        self,
        original_agg_map: dict[str, set[str]],
        clearing: Model | dict[str, Component],
        graph_clearing: dict[str, Component],
        aggregated: Model | dict[str, Component],
        graph_aggregated: dict[str, Component],
    ) -> dict[str, set[str]]:
        """Create aggregation map with simpler Component IDs based on an original mapping from clearing to aggregated.

        Use get_top_parent of components to find IDs in original_agg_map then change to the Flow/Node ID.

        Args:
            original_agg_map (dict[str, set[str]]): Mapping between Components of clearing and aggregated Models.
            clearing (dict[str, Component]): Clearing Model with top parents.
            graph_clearing (dict[str, Component]): Clearing Model version with Flows and Nodes. Derived from
                                                     clearing.
            aggregated (dict[str, Component]): Aggregated Model with top parents. Aggregated from clearing.
            graph_aggregated (dict[str, Component]): Aggregated Model version with Flows and Nodes. Derived from
                                                       aggregated.

        Returns:
            dict[str, set[str]]: Mapping between components of simpler clearing and simpler aggregated Models.

        """
        if isinstance(clearing, Model):
            clearing = {k: v for k, v in clearing.get_data().items() if isinstance(v, Component)}
        if isinstance(aggregated, Model):
            aggregated = {k: v for k, v in aggregated.get_data().items() if isinstance(v, Component)}

        self._check_agg_map_compatibility(clearing, aggregated, original_agg_map)

        graph_clearing_map = self._get_top_parent_to_simple(original=clearing, simpler=graph_clearing)
        graph_aggregated_map = self._get_top_parent_to_simple(original=aggregated, simpler=graph_aggregated)
        simple_agg_map = {}

        for clearing_id, agg_ids in original_agg_map.items():
            # the two if statements are there for if we want to map only a subset of the simpler Components.
            if clearing_id in graph_clearing_map:
                if not agg_ids:
                    continue  # choose not to add deleted components. May change this later.
                simple_agg_ids = set()
                for agg_id in agg_ids:
                    if agg_id in graph_aggregated_map:  # Again to allow subset to be mapped
                        simple_agg_ids |= graph_aggregated_map[agg_id]  # add set if simple component ids
                if simple_agg_ids:
                    for graph_clearing_id in graph_clearing_map[clearing_id]:
                        simple_agg_map[graph_clearing_id] = simple_agg_ids

        self._check_agg_map_validity(graph_clearing, graph_aggregated, simple_agg_map)
        return simple_agg_map

    def _check_agg_map_compatibility(
        self,
        clearing: Model | dict[str, Component],
        aggregated: Model | dict[str, Component],
        original_agg_map: dict[str, set[str]],
    ) -> None:
        if set(clearing.keys()) != set(original_agg_map.keys()):
            missing_in_clearing = set(original_agg_map.keys()).difference(clearing.keys())
            extra_in_clearing = set(clearing.keys()).difference(original_agg_map.keys())
            message = (
                "clearing is incompatible with the aggregation mapping between clearing and aggregated Models.\n"
                f"Missing in clearing: {missing_in_clearing}\n"
                f"Extra in clearing: {extra_in_clearing}"
            )
            raise KeyError(message)

        original_agg_map_values = set().union(*(v for v in original_agg_map.values() if v))
        if set(aggregated.keys()) != original_agg_map_values:
            missing_in_aggregated = original_agg_map_values.difference(aggregated.keys())
            extra_in_aggregated = set(aggregated.keys()).difference(original_agg_map_values)
            message = (
                "aggregated is incompatible with the aggregation mapping between clearing and aggregated Models.\n"
                f"Missing in aggregated: {missing_in_aggregated}\n"
                f"Extra in aggregated: {extra_in_aggregated}"
            )
            raise KeyError(message)

    def _check_agg_map_validity(
        self,
        original_components: dict[str, Component],
        aggregated_components: dict[str, Component],
        agg_map: dict[str, set[str] | None],
    ) -> None:
        """Check Flow and Node rules for all mappings in an aggregation map."""
        errors = set()
        for original_id, aggregated_ids in agg_map.items():
            component = original_components[original_id]
            if isinstance(component, Node):
                self._check_node_rules(original_id, component, aggregated_ids, aggregated_components, errors)

            if isinstance(component, Flow) and component.get_startupcost() is not None:
                self._check_flow_rules(original_id, aggregated_ids, aggregated_components, errors)

            if not isinstance(component, (Flow, Node)):
                message = (
                    f"Invalig Model of simpler Components. Must consist of only Flows and Nodes. Found: {component}"
                )
                raise ValueError(message)

        self._report_errors(errors)

    def _check_node_rules(
        self,
        original_id: str,
        node: Node,
        aggregated_ids: set[str] | None,
        aggregated_components: dict[str, Component],
        errors: set[str],
    ) -> None:
        """Check rules for Nodes for a Component ID in an aggregation map.

        A Node on the disaggregated side (keys) must map to exactly one other Node. More keys are alowed to map to the
        same aggregated Node.

        """
        if node.get_storage() is None:
            # Check rules here?
            return

        if aggregated_ids is None:
            e = f"Node with Storage {original_id} was deleted during aggregations. This is not supported in JulES."
            errors.add(e)
            return
        aggregated_storages = set()
        for agg_id in aggregated_ids:
            agg_component = aggregated_components[agg_id]
            if isinstance(agg_component, Node) and agg_component.get_storage() is not None:
                aggregated_storages.add(agg_id)
        if len(aggregated_storages) != 1:
            errors.add(
                f"Node with Storage {original_id} must be connected to exactly one Node with Storage in the "
                f"aggregation map in JulES. Currently connected to: {aggregated_storages}.",
            )

    def _check_flow_rules(
        self,
        original_id: str,
        aggregated_ids: set[str] | None,
        aggregated_components: dict[str, Component],
        errors: set[str],
    ) -> None:
        """Check rules for Flows for a Component ID in an aggregation map.

        A Flow on the disaggregated side (keys) must map to exactly one other Flow. More keys are alowed to map to the
        same aggregated Flow.

        """
        if aggregated_ids is None:
            e = f"Flow with StartUpCost {original_id} was deleted during aggregations. This is not supported in JulES."
            errors.add(e)
            return
        aggregated_flows = set()
        for agg_id in aggregated_ids:
            agg_component = aggregated_components[agg_id]
            if isinstance(agg_component, Flow) and agg_component.get_startupcost() is not None:
                aggregated_flows.add(agg_id)
        if len(aggregated_flows) != 1:
            errors.add(
                f"Flow with StartUpCost {original_id} must be connected to exactly one Flow with StartUpCost in the "
                f"aggregation map in JulES. Currently connected to: {aggregated_flows}.",
            )

    @staticmethod
    def _get_storages(simpler: dict[str, Component]) -> set[str]:
        nodes_with_storages = set()
        for n, c in simpler.items():
            if isinstance(c, Node) and c.get_storage() is not None:
                nodes_with_storages.add(n)
        return nodes_with_storages

    @staticmethod
    def _get_top_parent_to_simple(original: dict[str, Component], simpler: dict[str, Component]) -> dict[str, set[str]]:
        """Map simpler components to their top parent."""
        inv_original = {c: n for n, c in original.items()}
        simpler_map: dict[str, set[str]] = {}

        for simple_id, component in simpler.items():
            top_parent = component.get_top_parent()
            if top_parent is None:
                message = (
                    f"Component {component} with ID {simple_id} has no parents. This means it has not been "
                    "derived from original."
                )
                raise ValueError(message)
            try:
                top_parent_id = inv_original[top_parent]
            except KeyError as e:
                message = (
                    f"Component {top_parent} does not exist in original Model. This means simpler has not been "
                    "derived from original."
                )
                raise KeyError(message) from e
            if top_parent_id in simpler_map:
                # list has been set, wo we add the simple component id to the
                simpler_map[top_parent_id].add(simple_id)
            else:
                simpler_map[top_parent_id] = {simple_id}

        return simpler_map
