"""Config object for JulES solver."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from framcore.aggregators import Aggregator, HydroAggregator
from framcore.solvers import SolverConfig

from framjules import JulESTimeResolution

_SUPPORTED_AGGREGATORS = [HydroAggregator]


class JulESConfig(SolverConfig):
    """Class containing all config for JulES."""

    def __init__(self) -> None:
        """Create new JulESConfig object."""
        super().__init__()

        self._julia_exe_path: Path | str | None = None
        self._julia_env_path: Path | str | None = None
        self._julia_depot_path: Path | str | None = None

        self._branch_jules: str | None = None
        self._branch_tulipa: str | None = None

        self._short_term_aggregations: list[Aggregator] = []

        self._time_resolution = JulESTimeResolution()

        self._duration_clearing = timedelta(days=1)
        self._duration_short_term = timedelta(days=6)
        self._duration_medium_term = timedelta(days=364)
        self._duration_long_term = timedelta(days=364 * 5)

        self._market_resolution_clearing = timedelta(hours=1)
        self._market_resolution_short_term = timedelta(hours=2)

        self._market_num_blocks_medium_term = 5
        self._market_num_blocks_long_term = 4

        self._storage_resolution_clearing = timedelta(days=1)
        self._storage_resolution_short_term = timedelta(days=2)
        self._storage_resolution_medium_term = timedelta(days=7)
        self._storage_resolution_long_term = timedelta(days=28)

        self._short_term_storage_cutoff_hours = 10

        self._debug_short_opt_solver = False
        self._debug_med_opt_solver = False
        self._debug_long_opt_solver = False
        self._debug_end_value_opt_solver = False
        self._debug_subsystem_master_opt_solver = False
        self._debug_subsystem_sub_opt_solver = False
        self._debug_clearing_opt_solver = False
        self._is_cache_db = True
        self._skip_install_dependencies = False

        self._clearing_days = 2
        self._market_duration_minutes = 6 * 60
        self._storage_duration_minutes = 2 * 24 * 60
        self._lookahead_days = 5 * 365
        self._detail_level = "fast"

        self._skipmax_days = 6
        self._warn_skipmax_days = 32

    def set_skipmax_days(self, days: int) -> None:
        """Set number of days between calculation of medium and long term storage values.

        This can speed up a simulation. The cost is less good storage values. The longer between
        re-calculation of storage values, the bigger negative impact on simulation result quality.

        If skipmax_days = 6 and clearing_days = 2, JulES will calculate medium and long
        term storage values every 3rd simulation step.
        """
        self._check_type(days, int)
        self._check_int(days, lower_bound=0, upper_bound=None)
        if days > self._warn_skipmax_days:
            message = (
                "Unusually high value for skipmax_days: "
                f"Medium and long term storage values updated only {days}th day. "
                "This can give poor simulation results due to poor storage utilization."
            )
            self.send_warning_event(message)
        self._skipmax_days = days

    def get_skipmax_days(self) -> int:
        """Get number of days between calculation of medium and long term storage values."""
        return self._skipmax_days

    def is_skip_install_dependencies(self) -> bool:
        """Return True if install dependencies will be skipped during by JulES.solve."""
        return self._skip_install_dependencies

    def activate_skip_install_dependencies(self) -> None:
        """Tell JulES to not install dependencies, assuming they are already installed.

        Default is to install.
        """
        self._skip_install_dependencies = True

    def deactivate_skip_install_dependencies(self) -> None:
        """Tell JulES to install dependencies. (This is the default)."""
        self._skip_install_dependencies = False

    def is_cache_db(self) -> bool:
        """Return True if JulES is allowed to use a cache to store precomputed values while building."""
        return self._is_cache_db

    def activate_cache_db(self) -> None:
        """Activates use of cache db."""
        self._is_cache_db = True

    def deactivate_cache_db(self) -> None:
        """Activates use of db without cache."""
        self._is_cache_db = False

    def get_time_resolution(self) -> JulESTimeResolution:
        """Get time resolution object. Modify this to modify time resolution of JulES."""
        return self._time_resolution

    def get_short_term_storage_cutoff_hours(self) -> int:
        """Return num hours.

        JulES will classify all storage subsystems with
        max storage duration less than cutoff as short term storage.
        """
        return self._short_term_storage_cutoff_hours

    def set_jules_version(self, jules_branch: str | None = None, tulipa_branch: str | None = None) -> None:
        """Set which git branch of JulES and/or TuLiPa to use."""
        self._check_type(jules_branch, (str, type(None)))
        self._check_type(tulipa_branch, (str, type(None)))
        if jules_branch is not None:
            self._branch_jules = jules_branch
        if tulipa_branch is not None:
            self._branch_tulipa = tulipa_branch
        if self._branch_tulipa is None and self._branch_jules is not None:
            self._branch_tulipa = self._branch_jules

    def get_jules_version(self) -> str | None:
        """Get JulES git branch."""
        return self._branch_jules

    def get_tulipa_version(self) -> str | None:
        """Get TuLiPa git branch."""
        return self._branch_tulipa

    def set_julia_depot_path(self, path: Path) -> None:
        """Set folder where Julia installs new packages."""
        self._check_type(path, Path)
        self._julia_depot_path = path

    def get_julia_depot_path(self) -> Path | None:
        """Get folder where Julia installs new packages."""
        return self._julia_depot_path

    def set_julia_env_path(self, path: Path) -> None:
        """Set which Julia environment to use."""
        self._check_type(path, Path)
        self._julia_env_path = path

    def get_julia_env_path(self) -> Path | None:
        """Get Julia environment being used."""
        return self._julia_env_path

    def set_julia_exe_path(self, path: Path) -> None:
        """Set which Julia installation to use."""
        self._check_type(path, Path)
        self._julia_exe_path = path

    def get_julia_exe_path(self) -> Path | None:
        """Get Julia installation being used."""
        return self._julia_exe_path

    def _check_supported_aggregators(self, aggregators: list[Aggregator]) -> None:
        for aggr in aggregators:
            if not isinstance(aggr, tuple(_SUPPORTED_AGGREGATORS)):
                message = f"Aggregator of type {type(aggr)} is not supported in JulES. Supported types are: {_SUPPORTED_AGGREGATORS}"
                raise TypeError(message)

    def set_short_term_aggregations(self, aggregators: list[Aggregator]) -> None:
        """Set aggregations to create the short term model from clearing (the Model object being solved)."""
        self._check_supported_aggregators(aggregators)
        self._short_term_aggregations = aggregators

    def get_short_term_aggregations(self) -> list[Aggregator]:
        """Get aggregations to create the short term model from clearing (the Model object being solved)."""
        return self._short_term_aggregations

    """
    Debug optimization solvers of the different JulES problems.
    Helpful if problems are encountered during solving, most commonly infeasibility issues.
    Replaces TuLiPa.HiGHS_Prob with TuLiPa.JuMP_Prob which has better debugging features:
    - More checks while building the optimization problem
    - If infeasible, solve the problem again with relaxed constraints (with penalties) and return the broken constraints
    - Outputs the optimization problem with variable and constraint names from FRAM
    """

    def set_debug_all_opt_solver(self, debug: bool) -> None:
        """Set whether to debug all optimization solvers."""
        self._debug_short_opt_solver = debug
        self._debug_med_opt_solver = debug
        self._debug_long_opt_solver = debug
        self._debug_end_value_opt_solver = debug
        self._debug_subsystem_master_opt_solver = debug
        self._debug_subsystem_sub_opt_solver = debug
        self._debug_clearing_opt_solver = debug

    def set_debug_short_opt_solver(self, debug: bool) -> None:
        """Set whether to debug the short-term optimization solver."""
        self._debug_short_opt_solver = debug

    def get_debug_short_opt_solver(self) -> bool:
        """Get whether to debug the short-term optimization solver."""
        return self._debug_short_opt_solver

    def set_debug_med_opt_solver(self, debug: bool) -> None:
        """Set whether to debug the medium-term optimization solver."""
        self._debug_med_opt_solver = debug

    def get_debug_med_opt_solver(self) -> bool:
        """Get whether to debug the medium-term optimization solver."""
        return self._debug_med_opt_solver

    def set_debug_long_opt_solver(self, debug: bool) -> None:
        """Set whether to debug the long-term optimization solver."""
        self._debug_long_opt_solver = debug

    def get_debug_long_opt_solver(self) -> bool:
        """Get whether to debug the long-term optimization solver."""
        return self._debug_long_opt_solver

    def set_debug_end_value_opt_solver(self, debug: bool) -> None:
        """Set whether to debug the end value optimization solver."""
        self._debug_end_value_opt_solver = debug

    def get_debug_end_value_opt_solver(self) -> bool:
        """Get whether to debug the end value optimization solver."""
        return self._debug_end_value_opt_solver

    def set_debug_subsystem_master_opt_solver(self, debug: bool) -> None:
        """Set whether to debug the subsystem master optimization solver."""
        self._debug_subsystem_master_opt_solver = debug

    def get_debug_subsystem_master_opt_solver(self) -> bool:
        """Get whether to debug the subsystem master optimization solver."""
        return self._debug_subsystem_master_opt_solver

    def set_debug_subsystem_sub_opt_solver(self, debug: bool) -> None:
        """Set whether to debug the subsystem sub optimization solver."""
        self._debug_subsystem_sub_opt_solver = debug

    def get_debug_subsystem_sub_opt_solver(self) -> bool:
        """Get whether to debug the subsystem sub optimization solver."""
        return self._debug_subsystem_sub_opt_solver

    def set_debug_clearing_opt_solver(self, debug: bool) -> None:
        """Set whether to debug the clearing optimization solver."""
        self._debug_clearing_opt_solver = debug

    def get_debug_clearing_opt_solver(self) -> bool:
        """Get whether to debug the clearing optimization solver."""
        return self._debug_clearing_opt_solver
