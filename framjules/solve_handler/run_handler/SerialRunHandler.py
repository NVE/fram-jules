from pathlib import Path

from framcore.juliamodels import JuliaModel

from framjules import JulESConfig
from framjules.solve_handler.JulESNames import JulESNames


class SerialRunHandler(JuliaModel):
    """Handle running the JulES solver in serial simulation mode."""

    # ENV_NAME = "JulES_julia_env"

    def __init__(
        self,
        folder: Path,
        config: JulESConfig,
        names: JulESNames,
        dependencies: list[str | tuple[str, str | None]] | None = None,
    ) -> None:
        """Initialize JulES serial folder.

        The three parameters env_path, depot_path and julia_path sets environment variables for locations of your Julia
        environment, packages and language.
            - If user has not specified locations, the default is to use the current python/conda environment.
            - If a system installation of Python is used, the default is set to the current user location.

        Args:
            folder (Path): Location of JulES model dataset.
            config (JulESConfig): Simulaiton config.
            names (JulESNames): JulES namespace object.
            dependencies (list[str]): Julia packages dependencies. List of str, either package names or urls.

        """
        self._folder = folder
        self._config = config
        self._names = names
        self.ENV_NAME = self._names.JULIA_ENV_NAME
        super().__init__(
            julia_path=self._config.get_julia_exe_path(),
            env_path=self._config.get_julia_env_path(),
            depot_path=self._config.get_julia_depot_path(),
            dependencies=dependencies,
            skip_install_dependencies=config.is_skip_install_dependencies(),
        )

    def run(self) -> None:
        """Run JulES in Series mode."""
        data_year = self._config.get_data_period().get_start_time().year
        weather_year = self._config.get_weather_years()[0]

        config_path = self._folder / self._names.JULES_CONFIG
        output_path = self._folder / self._names.FILENAME_H5_OUTPUT

        def get_all_attrs(obj) -> dict:  # noqa: ANN001
            result = {}
            result.update(
                {k: v for k, v in obj.__class__.__dict__.items() if not k.startswith("__") and not callable(v)},
            )
            result.update(obj.__dict__)
            return result

        names_dict = get_all_attrs(self._names)
        filename_clearing = f"{self._names.ROOT_FILENAME_DATAELEMENTS}_{self._names.CLEARING}.json"
        filename_aggregated = f"{self._names.ROOT_FILENAME_DATAELEMENTS}_{self._names.AGGREGATED}.json"

        self._jl.seval(f"""
        using Distributed, YAML, HDF5
        config = YAML.load_file(\"{config_path.as_posix()}\")
        println("Add cores")
        const numcores = config["main"]["numcores"]
        if nprocs() < numcores
            addprocs(numcores - nprocs())
        end
        @show nprocs()
        println("Load JulES")
        @time @everywhere using JulES
        using Pkg; Pkg.status()
        """)

        self._jl.JulES.run_jules(
            config_path.as_posix(),
            data_year,
            weather_year,
            output_path.as_posix(),
            names_dict,
            filename_clearing,
            filename_aggregated,
        )
