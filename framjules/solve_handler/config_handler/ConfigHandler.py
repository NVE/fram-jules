import contextlib
import math
from abc import abstractmethod
from pathlib import Path

import yaml

from framjules import JulESConfig
from framjules.solve_handler.dataclasses import ComponentInfo, GraphInfos
from framjules.solve_handler.JulESNames import JulESNames


class ConfigHandler:
    """Responsible for implementing shared functionality in build method."""

    def __init__(
        self,
        folder: Path,
        config: JulESConfig,
        names: JulESNames,
        graph_infos: GraphInfos,
    ) -> None:
        """Responsible for building config file for JulES."""
        self.folder = folder
        self.config = config
        self.names = names
        self.graph_infos = graph_infos

    def configure(self) -> None:
        """Build JulES config and dump to YAML file."""
        config = self.get_default_config_dict()
        self._make_shrinkable(config)
        self.adapt_config_to_simulation_mode(config)
        self.adapt_to_dataset(config)  # TODO: Remove if JulES does not require this after all
        self.config_assert_no_missings(config)
        self.write_config_file(config)

    def is_clearing_only(self) -> bool:
        """Return True if dataset has no subsystems."""
        return all(not info.is_sss_member for info in self.graph_infos.clearing.values())

    def is_short_term_only(self) -> bool:
        """Return True if dataset has only short term storage subsystems."""
        has_sss = any(info.is_sss_member for info in self.graph_infos.clearing.values())
        no_long_term = all(info.is_sss_member and info.sss_is_short_term for info in self.graph_infos.clearing.values())
        return has_sss and no_long_term

    def adapt_to_dataset(self, config: dict) -> None:
        """Remove keys in config if dataset does require it."""
        n = self.names

        settings = config[n.PY_JULES_SETTINGS_NAME]

        if self.is_clearing_only():
            del settings[n.SCENARIO_GENERATION]
            del settings[n.SUBSYSTEMS]
            for sub_key in list(settings[n.PROBLEMS].keys()):
                if sub_key != n.CLEARING:
                    del settings[n.PROBLEMS][sub_key]
            del settings[n.RESULTS][n.SCENARIOS]
            del settings[n.RESULTS][n.STORAGEVALUES]
            settings[n.HORIZONS][n.COMMODITIES] = [n.MARKET]
            with contextlib.suppress(Exception):
                for sub_key in list(settings[n.HORIZONS].keys()):
                    if sub_key not in [n.CLEARING, n.COMMODITIES]:
                        del settings[n.HORIZONS][sub_key]
            with contextlib.suppress(Exception):
                del settings[n.HORIZONS][n.CLEARING][n.STORAGE_SYSTEM]
            del settings[n.TIME][n.PROB_TIME][n.PHASE_IN_TIME]
            del settings[n.TIME][n.PROB_TIME][n.PHASE_IN_DELTA_STEPS]
            del settings[n.TIME][n.PROB_TIME][n.PHASE_IN_DELTA_DAYS]

        elif self.is_short_term_only():
            del settings[n.SCENARIO_GENERATION]
            settings[n.HORIZONS][n.COMMODITIES] = [n.MARKET]
            for term in [n.CLEARING, n.SHORT, n.MED, n.LONG]:
                del settings[n.HORIZONS][term][n.STORAGE_SYSTEM]

        weather_year_start, num_scen_years = self.config.get_weather_years()
        weather_year_stop = weather_year_start + num_scen_years  # also check prognosis and evp scenarios
        if (
            n.SCENARIO_GENERATION in settings
            and settings[n.SCENARIO_GENERATION][n.STOCHASTIC][n.NUM_SCEN] >= weather_year_stop - weather_year_start
        ):
            del settings[n.SCENARIO_GENERATION]

    def _is_other_flow_market(self, x: ComponentInfo) -> bool:
        return x.is_flow and (not x.is_exogenous) and x.is_market_flow

    def _is_other_flow_hydro(self, x: ComponentInfo) -> bool:
        return x.is_flow and (not x.is_exogenous) and (not x.is_market_flow)

    def get_default_config_dict(self) -> dict:
        """Get default config."""
        n = self.names

        weather_year_start, num_scen_years = self.config.get_weather_years()
        weather_year_stop = weather_year_start + num_scen_years

        other_flows_market = [k for k, v in self.graph_infos.clearing.items() if self._is_other_flow_market(v)]
        other_flows_hydro = [k for k, v in self.graph_infos.clearing.items() if self._is_other_flow_hydro(v)]

        return {
            n.MAIN: {
                n.INPUT: str(self.folder),
                n.OUTPUT_PATH: str(self.folder),
                n.NUM_CORES: self.config.get_num_cpu_cores(),
                n.DATA_YEARS: n.MISSING_CONFIG,
                n.SCENARIO_YEARS: n.MISSING_CONFIG,
                n.WEEK_START: n.MISSING_CONFIG,
                n.NUM_SIM_YEARS: n.MISSING_CONFIG,
                n.SETTINGS: n.PY_JULES_SETTINGS_NAME,
                n.OUTPUT_INDEX: self.config_get_output_index(),
                n.DATETIME_FORMAT: n.DATETIME_FORMAT_JULESIO,
                n.OUTPUT_FORMAT: n.HDF5,
                n.EXTRA_STEPS: 0,
                n.ELASTIC: n.FALSE,
            },
            n.PY_JULES_SETTINGS_NAME: {
                n.TIME: {
                    n.WEATHER_YEAR_START: weather_year_start,
                    n.WEATHER_YEAR_STOP: weather_year_stop,
                    n.PROB_TIME: n.MISSING_CONFIG,
                    n.SKIPMAX: self._get_skipmax(),
                },
                n.PROBLEMS: {
                    n.PROGNOSIS: {
                        n.AGGSUPPLYN: 4,
                        n.RESIDUAL_AREA_LIST: [],
                        n.SHORT_TERM_STORAGE_CUTOFF_HOURS: self.config.get_short_term_storage_cutoff_hours(),
                        n.LONG: {
                            n.SOLVER: n.JUMP_HIGHS if self.config.get_debug_long_opt_solver() else n.HIGHS_SIMPLEX,
                        },
                        n.MED: {n.SOLVER: n.JUMP_HIGHS if self.config.get_debug_med_opt_solver() else n.HIGHS_SIMPLEX},
                        n.SHORT: {
                            n.SOLVER: n.JUMP_HIGHS
                            if self.config.get_debug_short_opt_solver()
                            else n.HIGHS_SIMPLEX_NO_WARMSTART,
                        },
                        n.STARTSTORAGES: {n.FUNCTION: n.AGG_STARTMAG_DICT},
                    },
                    n.ENDVALUE: {
                        n.SOLVER: n.JUMP_HIGHS if self.config.get_debug_end_value_opt_solver() else n.HIGHS_SIMPLEX,
                        n.STARTSTORAGES: {n.FUNCTION: n.STARTMAG_DICT},
                    },
                    n.STOCHASTIC: {
                        n.MAXCUTS: 13,
                        n.LB: -1000000000000.0,
                        n.RELTOL: 0.0001,
                        n.MASTER: {
                            n.SOLVER: n.JUMP_HIGHS
                            if self.config.get_debug_subsystem_master_opt_solver()
                            else n.HIGHS_SIMPLEX,
                        },
                        n.SUBS: {
                            n.SOLVER: n.JUMP_HIGHS
                            if self.config.get_debug_subsystem_sub_opt_solver()
                            else n.HIGHS_SIMPLEX,
                        },
                        n.STARTSTORAGES: {n.FUNCTION: n.STARTMAG_DICT},
                        n.DISTRIBUTION_METHOD_MP: n.BYSIZE,
                        n.DISTRIBUTION_METHOD_SP: n.WITHMP,
                    },
                    n.CLEARING: {
                        n.SOLVER: n.JUMP_HIGHS
                        if self.config.get_debug_clearing_opt_solver()
                        else n.HIGHS_SIMPLEX_SIP_NO_WARMSTART,
                        n.STATEDEPENDENT_PROD: n.FALSE,
                        n.STATEDEPENDENT_PUMP: n.FALSE,
                        n.HEADLOSSCOST: n.FALSE,
                    },
                },
                n.SCENARIO_GENERATION: {
                    n.STOCHASTIC: {
                        n.FUNCTION: n.INFLOW_CLUSTERING_METHOD,
                        n.NUM_SCEN: 7,
                        n.SCEN_DELTA_DAYS: 365,
                        n.PARTS: 4,
                    },
                },
                n.SUBSYSTEMS: {
                    n.FUNCTION: n.TWO_STORAGE_DURATION,
                    n.SHORT_TERM_STORAGE_CUTOFF_HOURS: self.config.get_short_term_storage_cutoff_hours(),
                    n.SHORT_STOCH_DURATION_HOURS: self._get_short_stoch_hours(),
                    n.LONG_STOCH_DURATION_DAYS: self._get_long_stoch_days(),
                    n.LONG_EV_DURATION_DAYS: self._get_long_ev_days(),
                },
                n.HORIZONS: {
                    n.COMMODITIES: [n.POWER, n.HYDRO],
                    n.LONG: {
                        n.TERM_DURATION_DAYS: self.config.get_time_resolution().get_long_days(),
                        n.HYDRO: {
                            n.FUNCTION: n.SEQUENTIAL_HORIZON,
                            n.PERIOD_DURATION_DAYS: self.config.get_time_resolution().get_long_storage_days(),
                        },
                        n.POWER: (
                            self._get_adaptive_horizon(
                                clusters=self.config.get_time_resolution().get_long_adaptive_blocks(),
                                unit_duration_hours=self.config.get_time_resolution().get_long_adaptive_hours(),
                            )
                            if self._has_exogenous_market_flows("long_term")
                            else {
                                n.FUNCTION: n.SEQUENTIAL_HORIZON,
                                # ok as fallback because long storage period is multiple of med storage period
                                n.PERIOD_DURATION_DAYS: self.config.get_time_resolution().get_med_storage_days(),
                            }
                        ),
                    },
                    n.MED: {
                        n.TERM_DURATION_DAYS: self.config.get_time_resolution().get_med_days(),
                        n.HYDRO: {
                            n.FUNCTION: n.SEQUENTIAL_HORIZON,
                            n.PERIOD_DURATION_DAYS: self.config.get_time_resolution().get_med_storage_days(),
                        },
                        n.POWER: (
                            self._get_adaptive_horizon(
                                clusters=self.config.get_time_resolution().get_med_adaptive_blocks(),
                                unit_duration_hours=self.config.get_time_resolution().get_long_adaptive_hours(),
                            )
                            if self._has_exogenous_market_flows("medium_term")
                            else {
                                n.FUNCTION: n.SEQUENTIAL_HORIZON,
                                n.PERIOD_DURATION_DAYS: 1,  # ok as fallback because we know we always have whole days
                            }
                        ),
                    },
                    n.SHORT: {
                        n.TERM_DURATION_HOURS: self._get_short_hours(),
                        n.HYDRO: {
                            n.FUNCTION: n.SEQUENTIAL_HORIZON,
                            n.PERIOD_DURATION_HOURS: self._get_short_storage_hours(),
                        },
                        n.POWER: {
                            n.FUNCTION: n.SEQUENTIAL_HORIZON,
                            n.PERIOD_DURATION_HOURS: self._get_short_market_hours(),
                        },
                    },
                    n.CLEARING: {
                        n.TERM_DURATION_HOURS: self._get_clearing_hours(),
                        n.HYDRO: {
                            n.FUNCTION: n.SEQUENTIAL_HORIZON,
                            n.PERIOD_DURATION_HOURS: self._get_clearing_storage_hours(),
                        },
                        n.POWER: {
                            n.FUNCTION: n.SEQUENTIAL_HORIZON,
                            n.PERIOD_DURATION_HOURS: self._get_clearing_market_hours(),
                        },
                    },
                },
                n.RESULTS: {
                    n.MAINRESULTS: n.ALL,
                    "hydrolevels_water": n.TRUE,
                    n.TIMES: n.TRUE,
                    n.SCENARIOS: n.FALSE,
                    n.MEMORY: n.FALSE,
                    n.STORAGEVALUES: n.TRUE,
                    n.STORAGEVALUES_ALL_PROBLEMS: n.FALSE,
                    n.OTHER_TERMS: {
                        n.VARS: {
                            n.MARKET: other_flows_market,
                            **({n.HYDRO: other_flows_hydro} if other_flows_hydro else {}),
                        },
                    },
                },
            },
        }

    def _make_shrinkable(self, config: dict) -> None:
        # TODO: make less hard coded
        two_model_years = 364 * 2
        if self.config.get_time_resolution().get_long_days() < two_model_years:
            return

        shrinkafter_days = self.config.get_time_resolution().get_clearing_days()
        shrinkafter_days += self.config.get_time_resolution().get_short_days()
        shrinkafter_days += self.config.get_time_resolution().get_med_days()

        shrinkatleast_days = 360

        n = self.names
        config[n.PY_JULES_SETTINGS_NAME][n.HORIZONS][n.SHRINKABLE] = {
            n.SHRINKAFTER_DAYS: shrinkafter_days,
            n.SHRINKATLEAST_DAYS: shrinkatleast_days,
        }
        config[n.PY_JULES_SETTINGS_NAME][n.HORIZONS][n.LONG][n.SHRINKABLE] = n.TRUE

    def _get_clearing_hours(self) -> int:
        return int(self.config.get_time_resolution().get_clearing_days() * 24)

    def _get_clearing_market_hours(self) -> int:
        return int(self.config.get_time_resolution().get_clearing_market_minutes() / 60)

    def _get_clearing_storage_hours(self) -> int:
        return int(self.config.get_time_resolution().get_clearing_storage_minutes() / 60)

    def _get_short_hours(self) -> int:
        return int(self.config.get_time_resolution().get_short_days() * 24)

    def _get_short_market_hours(self) -> int:
        return int(self.config.get_time_resolution().get_short_market_minutes() / 60)

    def _get_short_storage_hours(self) -> int:
        return int(self.config.get_time_resolution().get_short_storage_minutes() / 60)

    def _get_long_ev_days(self) -> int:
        days = self.config.get_time_resolution().get_clearing_days()
        days += self.config.get_time_resolution().get_short_days()
        days += self.config.get_time_resolution().get_med_days()
        days += self.config.get_time_resolution().get_ev_days()
        return days

    def _get_long_stoch_days(self) -> int:
        days = self.config.get_time_resolution().get_clearing_days()
        days += self.config.get_time_resolution().get_short_days()
        days += self.config.get_time_resolution().get_med_days()
        return days

    def _get_short_stoch_hours(self) -> int:
        hours = self._get_clearing_hours()
        hours += self._get_short_hours()
        return hours

    def write_config_file(self, config: dict) -> None:
        """Write config.yml into folder."""
        # TODO: Add comments behind each config name?
        with Path.open(self.folder / self.names.FILENAME_CONFIG, "w") as f:
            yaml.dump(config, f, indent=self.names.YAML_INDENT)

    def config_get_output_index(self) -> str:
        """Which time dimension to use in output files.

        If serial simulation, scenario time is used.
        If prognosis simulation, data time is used.
        """
        if self.config.is_simulation_mode_serial():
            return self.names.WEATHER_YEAR
        # TODO add below when prognosis is supported:
        # if self.config.is_simulation_mode_prognosis():
        #     return self.names.DATA_YEAR
        raise NotImplementedError

    def _get_adaptive_horizon(self, clusters: int, unit_duration_hours: int) -> dict:
        n = self.names
        return {
            n.FUNCTION: n.ADAPTIVE_HORIZON,
            n.MACRO: n.STORAGE_SYSTEM,
            n.RHSDATA: {
                # TODO: support user supplied variation
                # n.FUNCTION: n.DYNAMIC_EXOGEN_PRICE_AH_DATA,
                # n.BALANCE_RHSDATA: self.names.dummy_exogenous_balance_name,
                n.FUNCTION: n.DYNAMIC_RHS_AH_DATA,
                n.COMMODITY: n.MARKET,
            },
            n.RHSMETHOD: n.KMEANS_AH_METHOD,
            n.CLUSTERS: clusters,
            n.UNIT_DURATION_HOURS: unit_duration_hours,
        }

    def _has_exogenous_market_flows(self, graph: str) -> bool:
        assert graph in ["clearing", "short_term", "medium_term", "long_term"]
        graphs_info = getattr(self.graph_infos, graph)
        return any(x.is_flow and x.is_exogenous and x.is_market_flow for x in graphs_info.values())

    @abstractmethod
    def adapt_config_to_simulation_mode(self, config: dict) -> None:
        """Add simulation mode spesific modifications to config."""
        pass

    def config_assert_no_missings(self, value: object) -> None:
        """RuntimeError if config containes MISSING_CONFIG after adapt_config_to_simulation_mode."""
        errors = set()
        self._config_assert_no_missings(errors, value, "")
        if errors:
            raise RuntimeError("\n".join(errors))

    def _config_assert_no_missings(self, errors: set[str], value: object, key: str) -> None:
        if isinstance(value, dict):
            for k, v in value.items():
                self._config_assert_no_missings(errors, v, k)
        if value == self.names.MISSING_CONFIG:
            message = f"Required config {key} not set."
            errors.add(message)

    def _get_skipmax(self) -> int:
        clearing_days = self.config.get_time_resolution().get_clearing_days()
        skipmax_days = self.config.get_skipmax_days()
        if skipmax_days == 0:
            return 0
        return math.floor(skipmax_days / clearing_days)
