"""Functionality to reate data element to JulES."""

from datetime import datetime
from pathlib import Path

from framjules.solve_handler.dataclasses import ComponentInfo
from framjules.solve_handler.JulESNames import JulESNames


class DataElementAppender:
    """Used to generate list of data elements for JulES."""

    def __init__(self, names: JulESNames) -> None:
        """Initialize new DataElementAppender."""
        self.data_elements: list = []
        self.names: JulESNames = names

    def exogenous_balance(
        self,
        balance_id: str,
        commodiy: str,
        price_param_id: str | float,
    ) -> None:
        """Append exogenous balance data element."""
        self.data_elements.append(
            [
                self.names.BALANCE,
                self.names.EXOGENBALANCE,
                balance_id,
                [self.names.COMMODITY, commodiy],
                [self.names.PRICE, price_param_id],
            ],
        )

    def endogenous_balance(
        self,
        balance_id: str,
        commodiy: str,
    ) -> None:
        """Append endogenous balance data element."""
        self.data_elements.append(
            [
                self.names.BALANCE,
                self.names.BASEBALANCE,
                balance_id,
                [self.names.COMMODITY, commodiy],
            ],
        )

    def mean_series_param(
        self,
        param_id: str,
        level: str | float,
        profile: str | float,
    ) -> None:
        """Append mean series param data element."""
        self.data_elements.append(
            [
                self.names.PARAM,
                self.names.MEANSERIESPARAM,
                param_id,
                [self.names.LEVEL, level],
                [self.names.PROFILE, profile],
            ],
        )

    def base_flow(self, flow_id: str) -> None:
        """Append base flow data element."""
        self.data_elements.append([self.names.FLOW, self.names.BASEFLOW, flow_id])

    def lower_zero_capacity(
        self,
        lower_bound_id: str,
        is_flow: bool,
        flow_or_storage_id: str,
    ) -> None:
        """Append zero lower capacity data element."""
        self.data_elements.append(
            [
                self.names.CAPACITY,
                self.names.LOWERZEROCAPACITY,
                lower_bound_id,
                [self.names.WHICHCONCEPT, self.names.FLOW if is_flow else self.names.STORAGE],
                [self.names.WHICHINSTANCE, flow_or_storage_id],
            ],
        )

    def positive_capacity(
        self,
        bound_id: str,
        is_flow: bool,
        flow_or_storage_id: str,
        is_lower_bound: bool,
        param_id: str,
    ) -> None:
        """Append positive capacity data element."""
        self.data_elements.append(
            [
                self.names.CAPACITY,
                self.names.POSITIVECAPACITY,
                bound_id,
                [self.names.WHICHCONCEPT, self.names.FLOW if is_flow else self.names.STORAGE],
                [self.names.WHICHINSTANCE, flow_or_storage_id],
                [self.names.BOUNDKEY, self.names.BOUNDLOWER if is_lower_bound else self.names.BOUNDUPPER],
                [self.names.PARAM, param_id],
            ],
        )

    def unit_param(
        self,
        unit_param_id: str,
        series_param_id: str,
        info: ComponentInfo,
    ) -> None:
        """Append unit param data element."""
        self.data_elements.append(
            [
                self.names.PARAM,
                info.unit_param_type,
                unit_param_id,
                [self.names.PARAM, series_param_id],
            ],
        )

    def base_arrow(
        self,
        arrow_id: str,
        flow_id: str,
        balance_id: str,
        is_ingoing: bool,
        conversion: str | float,
    ) -> None:
        """Append base arrow data element."""
        self.data_elements.append(
            [
                self.names.ARROW,
                self.names.BASEARROW,
                arrow_id,
                [self.names.FLOW, flow_id],
                [self.names.BALANCE, balance_id],
                [self.names.DIRECTIONKEY, self.names.DIRECTIONIN if is_ingoing else self.names.DIRECTIONOUT],
                [self.names.CONVERSION, conversion],
            ],
        )

    def cost_term(
        self,
        cost_term_id: str,
        flow_or_storage_id: str,
        is_flow: str,
        is_cost: bool,
        cost: str | float,
    ) -> None:
        """Append cost term data element."""
        self.data_elements.append(
            [
                self.names.COST,
                self.names.COSTTERM,
                cost_term_id,
                [self.names.DIRECTIONKEY, self.names.DIRECTIONIN if is_cost else self.names.DIRECTIONOUT],
                [self.names.PARAM, cost],
                [self.names.WHICHCONCEPT, self.names.FLOW if is_flow else self.names.STORAGE],
                [self.names.WHICHINSTANCE, flow_or_storage_id],
            ],
        )

    def base_rhs_term(
        self,
        rhs_term_id: str,
        balance_id: str,
        is_ingoing: bool,
        unit_param_id: str,
    ) -> None:
        """Append base rhs term data element."""
        # TODO: Add residualhint
        self.data_elements.append(
            [
                self.names.RHSTERM,
                self.names.BASERHSTERM,
                rhs_term_id,
                [self.names.BALANCE, balance_id],
                [self.names.DIRECTIONKEY, self.names.DIRECTIONIN if is_ingoing else self.names.DIRECTIONOUT],
                [self.names.PARAM, unit_param_id],
            ],
        )

    def base_storage(
        self,
        storage_id: str,
        balance_id: str,
    ) -> None:
        """Append base storage data element."""
        self.data_elements.append(
            [
                self.names.STORAGE,
                self.names.BASESTORAGE,
                storage_id,
                [self.names.BALANCE, balance_id],
            ],
        )

    def global_eneq(
        self,
        global_eneq_id: str,
        balance_id: str,
        value: float,
    ) -> None:
        """Append global energy equivalent data element."""
        self.data_elements.append(
            [
                self.names.METADATA,
                self.names.GLOBALENEQ,
                global_eneq_id,
                [self.names.BALANCE, balance_id],
                [self.names.VALUE, value],
            ],
        )

    def ms_time_delta(
        self,
        time_delta_id: str,
        milliseconds: float,
    ) -> None:
        """Append ms time delta data element."""
        self.data_elements.append(
            [
                self.names.TIMEDELTA,
                self.names.MSTIMEDELTA,
                time_delta_id,
                [self.names.PERIOD, milliseconds],
            ],
        )

    def range_time_index(
        self,
        time_index_id: str,
        start_time: datetime,
        num_steps: int,
        time_delta_id: str,
    ) -> None:
        """Append range time index data element."""
        self.data_elements.append(
            [
                self.names.TIMEINDEX,
                self.names.RANGETIMEINDEX,
                time_index_id,
                [self.names.START, start_time.strftime(r"%Y-%m-%d %H:%M:%S")],
                [self.names.STEPS, num_steps],
                [self.names.DELTA, time_delta_id],
            ],
        )

    def base_table(
        self,
        table_id: str,
        path_table: str | Path,
        column_names: list[str],
    ) -> None:
        """Append base table data element."""
        self.data_elements.append(
            [
                self.names.TABLE,
                self.names.BASETABLE,
                table_id,
                [self.names.MATRIX, str(path_table)],
                [self.names.NAMES, column_names],
            ],
        )

    def column_time_values(
        self,
        time_values_id: str,
        table_id: str,
        column_id: str,
    ) -> None:
        """Append column time values data element."""
        self.data_elements.append(
            [
                self.names.TIMEVALUES,
                self.names.COLUMNTIMEVALUES,
                time_values_id,
                [self.names.TABLE, table_id],
                [self.names.NAME, column_id],
            ],
        )

    def rotating_time_vector(
        self,
        time_vector_id: str,
        time_index_id: str,
        time_values_id: str,
    ) -> None:
        """Append rotating time vector data element."""
        self.data_elements.append(
            [
                self.names.TIMEVECTOR,
                self.names.ROTATINGTIMEVECTOR,
                time_vector_id,
                [self.names.TIMEINDEX, time_index_id],
                [self.names.TIMEVALUES, time_values_id],
            ],
        )

    def one_year_time_vector(
        self,
        time_vector_id: str,
        time_index_id: str,
        time_values_id: str,
    ) -> None:
        """Append one year time vector data element."""
        self.data_elements.append(
            [
                self.names.TIMEVECTOR,
                self.names.ONEYEARTIMEVECTOR,
                time_vector_id,
                [self.names.TIMEINDEX, time_index_id],
                [self.names.TIMEVALUES, time_values_id],
            ],
        )
