from dataclasses import dataclass

from framcore import Model
from framcore.components import Flow, Node


@dataclass
class DomainModels:
    """Model instance for each term."""

    clearing: Model
    short_term: Model
    medium_term: Model
    long_term: Model


@dataclass
class NodeFlowGraphs:
    """Node-Flow representation of domain model components via get_supported_components."""

    clearing: dict[str, Node | Flow]
    short_term: dict[str, Node | Flow]
    medium_term: dict[str, Node | Flow]
    long_term: dict[str, Node | Flow]


@dataclass
class ComponentInfo:
    """All derived info we need during solve."""

    is_flow: bool | None = None
    is_node: bool | None = None
    is_storage_node: bool | None = None
    is_market_node: bool | None = None
    is_market_flow: bool | None = None
    is_sss_member: bool | None = None
    is_exogenous: bool | None = None

    has_storage_resolution: bool | None = None

    jules_commodity: str | None = None
    domain_commodity: str | None = None

    # sss = storage subsystem
    sss_id: str | None = None
    sss_market_commodity: str | None = None
    sss_members: set[str] | None = None
    sss_is_short_term: int | None = None
    sss_global_eneq_value: float | None = None
    sss_global_eneq_unit: str | None = None
    sss_initial_storage: float | None = None

    jules_balance_id: str | None = None
    jules_storage_id: str | None = None
    jules_global_eneq_id: str | None = None

    main_node_id: str | None = None
    num_arrows: int | None = None

    unit_price: str | None = None
    unit_stock: str | None = None
    unit_flow: str | None = None
    unit_flow_result: str | None = None
    unit_cost: str | None = None
    unit_coeffs: dict[str : str | None] | None = None
    unit_param_type: str | None = None
    unit_param_unit_flow: str | None = None
    unit_param_unit_stock: str | None = None

    agg_storage_node_id: str | None = None
    agg_market_node_id: str | None = None


@dataclass
class GraphInfos:
    """Hold all component info for all graphs."""

    clearing: dict[str, ComponentInfo]
    short_term: dict[str, ComponentInfo]
    medium_term: dict[str, ComponentInfo]
    long_term: dict[str, ComponentInfo]
