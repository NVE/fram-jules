"""JulES solver."""

import cProfile
from pathlib import Path
from time import time
import gc

from framcore import Model
from framcore.solvers import Solver
from framcore.loaders import Loader
from framcore.utils import add_loaders

from framjules import JulESConfig
from framjules.solve_handler.SolveHandler import SolveHandler

_PROFILE_PERFORMANCE = False


class JulES(Solver):
    """JulES solver."""

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
        # TODO: verify that market resolution is finer than storage

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
