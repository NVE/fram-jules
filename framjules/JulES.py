import cProfile
import gc
from pathlib import Path
from time import time

from framcore import Model
from framcore.loaders import Loader
from framcore.solvers import Solver
from framcore.utils import add_loaders

from framjules import JulESConfig
from framjules.solve_handler.SolveHandler import SolveHandler

_PROFILE_PERFORMANCE = False  # use cProfile to profile performance of different steps in the solve process


class JulES(Solver):
    """Solver class for JulES - a fundamental energy market simulation model for operational planning.

    The JulES solver takes a populated Model, and uses the configuration set by the user in JulESConfig to run
    JulES and read results back into the Model. The main steps of the JulES solver class are described below:
    1. Transform Model into JulES compatible Components (SolveHandler)
    2. Build JulES input files (BuildHandler)
    3. Configure JulES according to the JulESConfig (ConfigHandler)
    4. Run JulES (and setup Julia environment if needed) (RunHandler)
    5. Read results from JulES back into the Model (ResultsHandler)

    SolveHandler - Initialize
    - Transform the Model Components into JulES compatible Components. JulES uses the main Components
    Node and Flow to represent the energy system.
    - If Aggregators are set in JulEConfig.short_term_aggregations, make an aggregated version of Model. The JulES,
    algorithm has different problems that will use different Models. The clearing, stochastic subsystem and end
    value problems will use the full detailed Model, while the price prognosis problem will use the aggregated Model.
    SolveHandler also makes the mappings between the detailed and aggregated Models to couple the problems.
    - JulES only support the commodities Power, Hydro and Battery at the moment. All commodities in Model will be mapped
    to these. The main property of the commodity in JulES is the horizon (with type, duration and resolution). Power is
    the default commodity, and all commodities with no storage will be mapped to Power. Battery represents short-term
    storage commodities with a detailed time resolution, while Hydro represents long-term storage commodities with a
    coarser time resolution.
    - Identify storagesystems (e.g. watersheds or batteries), and identify if they are long-term or short-term
    storage systems. Storage systems are short-term if all storages in the subsystem have lower storage duration than
    JulESConfig.get_short_term_storage_cutoff_hours(). All storage subsystems in the same category
    (short-term or long-term) will get the same storage commodities and horizons, problem structure and end-condition
    type.
        - Short-term: StochSubsystem, startequalstop, no skipmed, Battery commodity, short horizon duration.
        - Long-term: EVP and/or StochSubsystem, endvalues from ppp, skipmed, Hydro commodity, long horizon duration.
    - TODO: This implementation is built around the first JulES version and will be improved in the future.
    We would like to add more tailored configurations for each storage system. Also make models for each problem and
    subsystem in JulES, not just a detailed and aggregated version of Model that JulES has to derive all problems from.

    BuildHandler - Build JulES input files.
    - Write JulES input files for the detailed and aggregated elements, together with their timevectors.
    - Write JulES input files for detailed and aggregated start storages.
    - Write JulES input file for the mapping between detailed and aggregated storages.

    ConfigHandler - Configure JulES according to the JulESConfig set by the user and the Model.
    - Simulation mode, simulation periods, weather years and scenario generation.
    - Number of CPU cores to use, parallelization settings and optimization solvers.
    - Problem structure and horizons for each problem and commodity. Horizon type, duration and resolution.
        - The problem structure will in most cases consist of the following, which are run for each simulation step:
            - Deterministic price prognosis problems for each weather scenario
            - Deterministic end value problems for each storage subsystem and weather scenario
            - Stochastic (two-stage) subsystems problem for each storage subsystem
            - Market clearing problem
        - Exception 1: If there are no storages in the system, only the market clearing problem will be run.
            TODO: Should also check if there are other constraints coupling time periods.
            Then we need the price prognosis problems.
        - Exception 2: If there is only exogenous market nodes only stochastic subsystem problems will not be run.
        - TODO: Improve configuration possibilities for the different problem structure cases.
    - Result settings.
    - Turn on or off various JulES features.

    See JulES documentation at https://nve.github.io/JulES/ for more.

    Methods:
        __init__(): Initializes the solver with default configuration.
        get_config(): Returns the internal configuration object for customization.
        solve(model: Model): Solves the given model using JulES. Parent class method (Solver).

    """

    def __init__(self) -> None:
        """Create new JulES solver with default config set."""
        super().__init__()
        self._config = JulESConfig()

    def get_config(self) -> JulESConfig:
        """Get internal config object. Modify this to configure JulES."""
        return self._config

    def _solve(
        self,
        folder: Path,
        model: Model,
    ) -> None:
        t0 = time()
        if _PROFILE_PERFORMANCE:
            profiler = cProfile.Profile()
            profiler.enable()
        handler = SolveHandler(folder, model, self.get_config())
        self.send_debug_event(f"SolveHandler time: {round(time() - t0, 2)} seconds")
        if _PROFILE_PERFORMANCE:
            profiler.disable()  # Stop profiling
            profiler.dump_stats("profile_solvehandler_init.prof")

        t = time()
        if _PROFILE_PERFORMANCE:
            profiler = cProfile.Profile()
            profiler.enable()
        handler.build()
        if _PROFILE_PERFORMANCE:
            profiler.disable()  # Stop profiling
            profiler.dump_stats("profile_solvehandler_build.prof")
        self.send_debug_event(f"build time: {round(time() - t, 2)} seconds")

        t = time()
        handler.configure()
        self.send_debug_event(f"configure time: {round(time() - t, 2)} seconds")

        t = time()
        loaders: set[Loader] = set()
        add_loaders(loaders, model)
        for loader in loaders:
            loader.clear_cache()
        gc.collect()
        self.send_debug_event(f"clear_cache time: {round(time() - t, 2)} seconds")

        t = time()
        handler.run()
        self.send_debug_event(f"run time: {round(time() - t, 2)} seconds")

        t = time()
        if _PROFILE_PERFORMANCE:
            profiler = cProfile.Profile()
            profiler.enable()
        handler.set_results()
        if _PROFILE_PERFORMANCE:
            profiler.disable()  # Stop profiling
            profiler.dump_stats("profile_solvehandler_results.prof")
        self.send_debug_event(f"set_results time: {round(time() - t, 2)} seconds")

        self.send_debug_event(f"JulES._solve time: {round(time() - t0, 2)} seconds")
