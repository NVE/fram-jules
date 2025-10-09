import pytest
from framcore.components import Component, Flow, Node

from framjules.solve_handler.JulESAggregator import JulESAggregator


class NodeWithStorage(Node):
    def __init__(self):
        pass

    def get_storage(self):
        return ""


class NodeWithoutStorage(Node):
    def __init__(self):
        pass

    def get_storage(self):
        return None


class FlowWithStartUpCost(Flow):
    def __init__(self):
        pass

    def get_startupcost(self):
        return ""


class FlowWithoutStartUpCost(Flow):
    def __init__(self):
        pass

    def get_startupcost(self):
        return None


# @pytest.mark.skip(reason="Skipping this test temporarily")
def test_create_aggregation_map():
    class TestAggregator:
        def __init__(self, agg_map: dict[str, str]):
            self._map = agg_map

        def get_aggregation_map(self) -> dict[str, str]:
            return self._map

    class TestModel:
        def __init__(self, dummy_data: dict[str, str]):
            self._data = dummy_data

        def get_data(self):
            return self._data

    class TestComponent(Component):
        def __init__(self):
            super().__init__()

    TestComponent.__abstractmethods__ = None

    clearing = TestModel(
        {
            "Node1": TestComponent(),
            "Node2": TestComponent(),
            "Node3": TestComponent(),
            "Module1": TestComponent(),
            "Module2": TestComponent(),
            "Module3": TestComponent(),
            "Module4": TestComponent(),
            "Thermal1": TestComponent(),
            "Wind1": TestComponent(),
        },
    )
    short = [
        TestAggregator(
            {
                "Node1": {"OSTLAND", "SORLAND"},
                "Node2": {"OSTLAND"},
                "Node3": {"SORLAND"},
                "Module1": {"Module1"},
                "Module2": {"Module2"},
                "Module3": {"Module3"},
                "Module4": {"Module4"},
                "Thermal1": {"Thermal"},
            },
        ),
        TestAggregator(
            {
                "OSTLAND": {"OSTLAND"},
                "SORLAND": {"SORLAND"},
                "Module1": {"Module_OSTLAND"},
                "Module2": {"Module_OSTLAND", "Module_SORLAND"},
                "Module3": {"Module_SORLAND"},
                "Module4": {"Module_SORLAND"},
                "Thermal": set(),
            },
        ),
    ]

    jules_agg = JulESAggregator(clearing, [], [], [])

    result = jules_agg._create_aggregation_map(clearing, short)

    expected = {
        "Node1": {"OSTLAND", "SORLAND"},
        "Node2": {"OSTLAND"},
        "Node3": {"SORLAND"},
        "Module1": {"Module_OSTLAND"},
        "Module2": {"Module_OSTLAND", "Module_SORLAND"},
        "Module3": {"Module_SORLAND"},
        "Module4": {"Module_SORLAND"},
        "Thermal1": set(),
        "Wind1": {"Wind1"},
    }

    assert result == expected


# @pytest.mark.skip(reason="Skipping this test temporarily")
def test_get_simpler_aggregation_map():
    class TestComponent(Component):
        def __init__(self, parent):
            self._parent = parent

        def get_top_parent(self):
            return self._parent

    TestComponent.__abstractmethods__ = None

    class TestFlow(Flow):
        def __init__(self, parent):
            self._parent = parent

        def get_top_parent(self):
            return self._parent

        def get_startupcost(self):
            return None

    t1_clearing = TestComponent(None)
    simpler_t1_clearing = TestFlow(t1_clearing)

    t2_clearing = TestComponent(None)
    simpler_t2_clearing = TestFlow(t1_clearing)

    n1_clearing = TestComponent(None)
    simpler_n1_clearing = TestFlow(n1_clearing)

    m_clearing = TestComponent(None)
    simpler_m_clearing_1 = TestFlow(m_clearing)
    simpler_m_clearing_2 = TestFlow(m_clearing)

    t_aggregated = TestComponent(None)
    simpler_t_aggregated = TestFlow(t_aggregated)

    m1_aggregated = TestComponent(None)
    simpler_m1_aggregated_1 = TestFlow(m1_aggregated)
    simpler_m1_aggregated_2 = TestFlow(m1_aggregated)
    m2_aggregated = TestComponent(None)
    simpler_m2_aggregated_1 = TestFlow(m2_aggregated)
    simpler_m2_aggregated_2 = TestFlow(m2_aggregated)

    jules_agg = JulESAggregator("dummy", [], [], [])

    clearing = {"T1": t1_clearing, "T2": t2_clearing, "N1": n1_clearing, "M": m_clearing}
    aggregated = {"T": t_aggregated, "N1": n1_clearing, "M1": m1_aggregated, "M2": m2_aggregated}

    clearing_simpler = {
        "T1_simpler": simpler_t1_clearing,
        "T2_simpler": simpler_t2_clearing,
        "N1_simpler": simpler_n1_clearing,
        "M_simpler1": simpler_m_clearing_1,
        "M_simpler2": simpler_m_clearing_2,
    }
    aggregated_simpler = {
        "T_simpler": simpler_t_aggregated,
        "M1_simpler1": simpler_m1_aggregated_1,
        "M1_simpler2": simpler_m1_aggregated_2,
        "M2_simpler1": simpler_m2_aggregated_1,
        "M2_simpler2": simpler_m2_aggregated_2,
    }

    agg_map = {"T1": {"T"}, "T2": {"T"}, "N1": {"N1"}, "M": {"M1", "M2"}}

    result = jules_agg._get_graph_aggregation_map(
        original_agg_map=agg_map,
        clearing=clearing,
        graph_clearing=clearing_simpler,
        aggregated=aggregated,
        graph_aggregated=aggregated_simpler,
    )

    expected = {
        "T1_simpler": {"T_simpler"},
        "T2_simpler": {"T_simpler"},
        "M_simpler1": {"M1_simpler1", "M2_simpler1", "M1_simpler2", "M2_simpler2"},
        "M_simpler2": {"M1_simpler1", "M2_simpler1", "M1_simpler2", "M2_simpler2"},
    }
    assert result == expected


def test_assert_assert_equal_storages_fails_when_unequal():
    simpler_short = {"N1_s": NodeWithStorage(), "N2_s": NodeWithStorage(), "N1": NodeWithoutStorage()}
    simpler_medium = {"N1_s": NodeWithStorage(), "N": NodeWithoutStorage(), "N3_s": NodeWithStorage()}
    simpler_long = {"N_s": NodeWithStorage()}

    expected_unique_short = {"N2_s"}
    expected_unique_medium = {"N3_s"}
    expected_unique_long = {"N_s"}

    jules_aggregator = JulESAggregator("dummy", [], [], [])
    expected_message = "Storages are not equal between short, medium and long term Models."
    expected_message += f"\n - Unique Nodes with Storages in Short Model: {expected_unique_short}"
    expected_message += f"\n - Unique Nodes with Storages in Medium Model: {expected_unique_medium}"
    expected_message += f"\n - Unique Nodes with Storages in Long Model: {expected_unique_long}"
    with pytest.raises(ValueError, match=expected_message):
        jules_aggregator.assert_equal_storages(simpler_short, simpler_medium, simpler_long)


def test_assert_assert_equal_storages_passes_when_equal():
    """Test case where method passes since storages in Models are equal."""

    class TestNodeWithStorage(Node):
        def __init__(self):
            pass

        def get_storage(self):
            return ""

    class TestNodeWithoutStorage(Node):
        def __init__(self):
            pass

        def get_storage(self):
            return None

    simpler_short = {
        "N1_s": TestNodeWithStorage(),
        "N2_s": TestNodeWithStorage(),
        "N1": TestNodeWithoutStorage(),
        "N2": TestNodeWithoutStorage(),
    }
    simpler_medium = {"N1_s": TestNodeWithStorage(), "N2_s": TestNodeWithStorage(), "N": TestNodeWithoutStorage()}
    simpler_long = {"N_s": TestNodeWithStorage(), "N2_s": TestNodeWithStorage()}

    jules_aggregator = JulESAggregator("dummy", [], [], [])

    try:
        jules_aggregator.assert_equal_storages(simpler_short, simpler_medium, simpler_long)
    except ValueError:
        message = (
            "Test failed because JulESAggregator.assert_equal_storage raised ValueError when Storages of the "
            "test case Models are equal."
        )
        pytest.fail(
            message,
            pytrace=True,
        )


def test_check_node_rules_adds_one_to_many_errors():
    node_id = "original_node"
    node = NodeWithStorage()

    aggregated_ids = {"aggnode1", "aggnode2", "aggnode3"}
    aggregated_components = {
        "aggnode1": NodeWithStorage(),
        "aggnode2": NodeWithStorage(),
        "aggnode3": NodeWithoutStorage(),
    }

    jules_aggregator = JulESAggregator("dummy", [], [], [])

    result: set[str] = set()
    jules_aggregator._check_node_rules(node_id, node, aggregated_ids, aggregated_components, result)

    expected = (
        f"Node with Storage {node_id} must be connected to exactly one Node with Storage in the "
        f"aggregation map in JulES. Currently connected to:"
    )
    result_element = next(iter(result))
    assert isinstance(result_element, str)
    assert len(result) == 1
    assert result_element.startswith(expected)
    assert "aggnode1" in result_element
    assert "aggnode2" in result_element


def test_check_node_rules_adds_deleted_errors():
    node_id = "original_node"
    node = NodeWithStorage()

    aggregated_ids = None
    aggregated_components = {
        "aggnode1": NodeWithStorage(),
        "aggnode2": NodeWithStorage(),
        "aggnode3": NodeWithoutStorage(),
    }

    jules_aggregator = JulESAggregator("dummy", [], [], [])

    result = set()
    jules_aggregator._check_node_rules(node_id, node, aggregated_ids, aggregated_components, result)

    expected = {f"Node with Storage {node_id} was deleted during aggregations. This is not supported in JulES."}

    assert result == expected


def test_check_flow_rules_adds_one_to_many_errors():
    flow_id = "original_flow"

    aggregated_ids = {"aggflow1", "aggflow2", "aggflow3"}
    aggregated_components = {
        "aggflow1": FlowWithStartUpCost(),
        "aggflow2": FlowWithStartUpCost(),
        "aggflow3": FlowWithoutStartUpCost(),
    }

    jules_aggregator = JulESAggregator("dummy", [], [], [])

    result = set()
    jules_aggregator._check_flow_rules(flow_id, aggregated_ids, aggregated_components, result)

    expected_connections = {"aggflow1", "aggflow2"}
    expected = {
        f"Flow with StartUpCost {flow_id} must be connected to exactly one Flow with StartUpCost in the "
        f"aggregation map in JulES. Currently connected to: {expected_connections}.",
    }

    assert result == expected


def test_check_flow_rules_adds_deleted_errors():
    flow_id = "original_flow"

    aggregated_ids = None
    aggregated_components = {
        "aggflow1": FlowWithStartUpCost(),
        "aggflow2": FlowWithStartUpCost(),
        "aggflow3": FlowWithoutStartUpCost(),
    }

    jules_aggregator = JulESAggregator("dummy", [], [], [])

    result = set()
    jules_aggregator._check_flow_rules(flow_id, aggregated_ids, aggregated_components, result)

    expected = {f"Flow with StartUpCost {flow_id} was deleted during aggregations. This is not supported in JulES."}

    assert result == expected
