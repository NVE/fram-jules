import datetime
import math

from framcore.timeindexes import SinglePeriodTimeIndex

from framjules.solve_handler.config_handler.ConfigHandler import ConfigHandler


class SerialConfigHandler(ConfigHandler):
    """Specialized methods for serial simulation."""

    def adapt_config_to_simulation_mode(self, config: dict) -> None:
        """Add serial simulation mode spesific modifications to config."""
        n = self.names

        data_years, scen_years, num_sim_steps = self.config_get_years_and_num_sim_steps()

        main_config = config[n.MAIN]
        main_config[n.DATA_YEARS] = data_years
        main_config[n.SCENARIO_YEARS] = scen_years
        main_config[n.WEEK_START] = 1
        main_config[n.NUM_SIM_YEARS] = 0
        main_config[n.EXTRA_STEPS] = num_sim_steps

        settings = config[n.PY_JULES_SETTINGS_NAME]
        settings[n.TIME][n.PROB_TIME] = {
            n.NORMAL_TIME: n.FIXED_DATA_TWO_TIME,
            n.PHASE_IN_TIME: n.PHASE_IN_FIXED_DATA_TWO_TIME,
            n.PHASE_IN_DELTA_DAYS: 7,
            n.PHASE_IN_DELTA_STEPS: 7,
        }

    def config_get_years_and_num_sim_steps(self) -> tuple:
        """Get data, scen and sim period config for serial simulation."""
        data_period = self.config.get_data_period()
        assert isinstance(data_period, SinglePeriodTimeIndex)
        data_years: list[int] = [data_period.get_start_time().isocalendar().year]

        first_scen_year, num_scen_years = self.config.get_weather_years()
        first_sim_year, num_sim_years = self.config.get_simulation_years()
        assert first_scen_year <= first_sim_year
        assert first_scen_year + num_scen_years >= first_sim_year + num_sim_years
        scen_years: list[int] = [first_sim_year]

        sim_start = datetime.datetime.fromisocalendar(first_sim_year, 1, 1)
        sim_stop = datetime.datetime.fromisocalendar(first_sim_year + num_sim_years, 1, 1)

        clearing_days = self.config.get_time_resolution().get_clearing_days()
        num_sim_steps = math.ceil((sim_stop - sim_start).total_seconds() / (clearing_days * 24 * 3600))

        return data_years, scen_years, num_sim_steps
