class JulESNames:
    """Constants (both static and dynamic ones defined in __init__) used in JulES."""

    JSON_INDENT = 4
    YAML_INDENT = 4

    OTHER_TERMS = "otherterms"
    VARS = "Vars"

    AGGREGATED = "aggregated"
    MACRO = "macro"
    BALANCE_RHSDATA = "balance"

    FILENAME_CONFIG = "config.yaml"
    FILENAME_H5_OUTPUT = "output.h5"
    FILENAME_STORAGE_MAPPING = "storage_mapping.json"
    FILENAME_START_STORAGES_AGGREGATED = "start_storages_aggregated.json"
    FILENAME_START_STORAGES_CLEARING = "start_storages_clearing.json"
    ROOT_FILENAME_DATAELEMENTS = "data_elements"
    FILENAME_DATAELEMENTS_TIMEVECTORS = "data_elements_timevectors.json"

    PY_JULES_SETTINGS_NAME = "python_jules_settings"
    PY_JULES_OUTPUT_NAME = "python_jules_output"

    MAIN = "main"
    MISSING_CONFIG = "missing_config"

    RESULTS = "results"
    MAINRESULTS = "mainresults"
    TIMES = "times"
    SCENARIOS = "scenarios"
    MEMORY = "memory"
    STORAGEVALUES = "storagevalues"
    STORAGEVALUES_ALL_PROBLEMS = "storagevalues_all_problems"
    ALL = "all"

    TERM_DURATION_WEEKS = "termduration_weeks"
    TERM_DURATION_DAYS = "termduration_days"
    TERM_DURATION_HOURS = "termduration_hours"
    SEQUENTIAL_HORIZON = "SequentialHorizon"
    ADAPTIVE_HORIZON = "AdaptiveHorizon"

    COMMODITIES = "commodities"
    POWER = "Power"
    HYDRO = "Hydro"
    BATTERY = "Battery"

    SHRINKAFTER_DAYS = "startafter_days"
    SHRINKATLEAST_DAYS = "shrinkatleast_days"

    TWO_STORAGE_DURATION = "twostorageduration"
    SHORT_STOCH_DURATION_HOURS = "shorttermstoragecutoff_hours"
    LONG_STOCH_DURATION_DAYS = "longstochduration_days"
    LONG_EV_DURATION_DAYS = "longevduration_days"

    DISTRIBUTION_METHOD_MP = "distribution_method_mp"
    DISTRIBUTION_METHOD_SP = "distribution_method_sp"
    BYSIZE = "bysize"
    ADVANCED = "advanced"
    STORAGE = "storage"
    GREEDY = "greedy"
    WITHMP = "withmp"

    STATEDEPENDENT_PROD = "statedependentprod"
    STATEDEPENDENT_PUMP = "statedependentpump"
    HEADLOSSCOST = "headlosscost"

    SUBSYSTEMS = "subsystems"
    RESULTS = "results"
    STARTSTORAGES = "startstorages"
    ENDVALUE = "endvalue"

    OUTPUT_FORMAT = "outputformat"
    DATETIME_FORMAT = "datetimeformat"
    DATETIME_FORMAT_JULESIO = "yyyy-mm-ddTHH:MM:SS"
    HDF5 = "hdf5"
    ELASTIC = "elastic"
    TRUE = True
    FALSE = False

    JULIA = "julia"
    INPUT = "input"
    OUTPUT_PATH = "outputpath"
    NUM_CORES = "numcores"
    DATA_YEARS = "datayears"
    SCENARIO_YEARS = "weatheryears"
    WEEK_START = "weekstart"
    NUM_SIM_YEARS = "simulationyears"
    EXTRA_STEPS = "extrasteps"
    SETTINGS = "settings"
    OUTPUT_NAME = "outputname"

    OUTPUT_INDEX = "outputindex"
    WEATHER_YEAR = "weatheryear"
    DATA_YEAR = "datayear"

    TIME = "time"
    WEATHER_YEAR_START = "weatheryearstart"
    WEATHER_YEAR_STOP = "weatheryearstop"
    PROB_TIME = "probtime"
    NORMAL_TIME = "normaltime"

    FIXED_DATA_TWO_TIME = "FixedDataTwoTime"
    PHASE_IN_FIXED_DATA_TWO_TIME = "PhaseinFixedDataTwoTime"

    PHASE_IN_TIME = "phaseintime"
    PHASE_IN_DELTA_DAYS = "phaseindelta_days"
    PHASE_IN_DELTA_STEPS = "phaseinsteps"
    PROBLEMS = "problems"
    PROGNOSIS = "prognosis"
    SIMULATION = "simulation"
    SHRINKABLE = "shrinkable"
    AGGZONE = "aggzone"
    AGGSUPPLYN = "aggsupplyn"
    SHORT_TERM_STORAGE_CUTOFF_HOURS = "shorttermstoragecutoff_hours"
    SHORTER_THAN_PROGNOSIS_MED_DAYS = "shorterthanprognosismed_days"
    LONG = "long"
    MED = "med"
    SHORT = "short"
    PROB = "prob"
    SOLVER = "solver"
    FUNCTION = "function"
    AGG_STARTMAG_DICT = "aggstartmagdict"
    STARTMAG_DICT = "startmagdict"
    RESIDUAL_AREA_LIST = "residualarealist"

    SCENARIO_GENERATION = "scenariogeneration"
    INFLOW_CLUSTERING_METHOD = "InflowClusteringMethod"
    NUM_SCEN = "numscen"
    SCEN_DELTA_DAYS = "scendelta_days"

    PARTS = "parts"

    SKIPMAX = "skipmax"

    HIGHS_PROB = "HiGHS_Prob()"
    HIGHS_SIMPLEX = "HighsSimplexMethod()"
    HIGHS_SIMPLEX_NO_WARMSTART = "HighsSimplexMethod(warmstart=false)"
    HIGHS_SIMPLEX_SIP_NO_WARMSTART = "HighsSimplexSIPMethod(warmstart=false)"
    JUMP_HIGHS = "JuMPHiGHSMethod()"

    STOCHASTIC = "stochastic"
    MAXCUTS = "maxcuts"
    LB = "lb"
    RELTOL = "reltol"
    ONLY_AGG_HYDRO = "onlyagghydro"
    MASTER = "master"
    SUBS = "subs"

    HORIZONS = "horizons"
    HORIZON_DURATION_WEEKS = "horizonduration_weeks"
    HORIZON_DURATION_HOURS = "horizonduration_hours"
    PERIOD_DURATION_DAYS = "periodduration_days"
    PERIOD_DURATION_HOURS = "periodduration_hours"
    POWER_PARTS = "powerparts"

    RHSDATA = "rhsdata"
    DYNAMIC_EXOGEN_PRICE_AH_DATA = "DynamicExogenPriceAHData"
    DYNAMIC_RHS_AH_DATA = "DynamicRHSAHData"
    RHSMETHOD = "rhsmethod"
    KMEANS_AH_METHOD = "KMeansAHMethod()"
    CLUSTERS = "clusters"
    UNIT_DURATION_HOURS = "unitduration_hours"

    SETTINGS_SCENARIO_YEAR_START = "scenarioyearstart"

    CLEARING = "clearing"
    SHORT_TERM = "short_term"
    MEDIUM_TERM = "medium_term"
    LONG_TERM = "long_term"

    MARKET = "Power"
    STORAGE_SYSTEM = "Hydro"
    SHORT_TERM_STORAGE = "Battery"

    DFMTin = "%Y-%m-%dT%H:%M:%S"

    FLOW = "Flow"
    STORAGE = "Storage"
    BALANCE = "Balance"
    COMMODITY = "Commodity"
    PARAM = "Param"
    CAPACITY = "Capacity"
    RHSTERM = "RHSTerm"
    TIMEVECTOR = "TimeVector"
    TIMEINDEX = "TimeIndex"
    TABLE = "Table"
    TIMEDELTA = "TimeDelta"
    TIMEVALUES = "TimeValues"
    ARROW = "Arrow"
    LOSS = "Loss"
    PRICE = "Price"
    CONVERSION = "Conversion"
    COST = "Cost"
    STARTUPCOST = "StartUpCost"

    BASEFLOW = "BaseFlow"
    BASEBALANCE = "BaseBalance"
    EXOGENBALANCE = "ExogenBalance"
    BASESTORAGE = "BaseStorage"
    MWTOGWHPARAM = "MWToGWhParam"
    M3STOMM3PARAM = "M3SToMM3Param"
    MEANSERIESPARAM = "MeanSeriesParam"
    MSTIMEDELTA = "MsTimeDelta"
    INFINITETIMEVECTOR = "InfiniteTimeVector"
    ROTATINGTIMEVECTOR = "RotatingTimeVector"
    ONEYEARTIMEVECTOR = "OneYearTimeVector"
    CONSTANTTIMEVECTOR = "ConstantTimeVector"
    RANGETIMEINDEX = "RangeTimeIndex"
    VECTORTIMEINDEX = "VectorTimeIndex"
    BASETABLE = "BaseTable"
    COLUMNTIMEVALUES = "ColumnTimeValues"
    VECTORTIMEVALUES = "VectorTimeValues"
    LOWERZEROCAPACITY = "LowerZeroCapacity"
    POSITIVECAPACITY = "PositiveCapacity"
    BASERHSTERM = "BaseRHSTerm"
    BASEARROW = "BaseArrow"
    SEGMENTEDARROW = "SegmentedArrow"
    SIMPLELOSS = "SimpleLoss"
    COSTTERM = "CostTerm"
    SIMPLESTARTUPCOST = "SimpleStartUpCost"

    LOSSFACTORKEY = "LossFactor"
    UTILIZATIONKEY = "Utilization"
    FALLBACK_UTILIZATION = 0.5

    STARTCOSTKEY = "StartCost"
    MINSTABLELOADKEY = "MinStableLoad"

    WHICHCONCEPT = "WhichConcept"
    WHICHINSTANCE = "WhichInstance"

    DIRECTIONKEY = "Direction"
    DIRECTIONIN = "In"
    DIRECTIONOUT = "Out"

    BOUNDKEY = "Bound"
    BOUNDUPPER = "Upper"
    BOUNDLOWER = "Lower"

    LEVEL = "Level"
    PROFILE = "Profile"
    VALUE = "Value"
    START = "Start"
    STEPS = "Steps"
    DELTA = "Delta"
    PERIOD = "Period"
    VECTOR = "Vector"
    MATRIX = "Matrix"
    NAMES = "Names"
    NAME = "Name"

    METADATA = "Metadata"
    GLOBALENEQ = "GlobalEneq"
    RESIDUALHINT = "Residualhint"

    JULES_CONFIG = "config.yaml"
    OUTPUT_FOLDER = "output"
    JULIA_ENV_NAME = "JulES_julia_env"

    def __init__(self) -> None:
        """Dynamically settable names for JulES."""
        # This is set in BuildHandler when we build data elements
        # for the clearing model. It is used in ConfigHandler in
        # connection with using AdaptiveHorizon
        self.dummy_exogenous_balance_name: str | None = None
        self.dummy_exogenous_profile_id: str | None = None
