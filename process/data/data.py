from process.data.query import obtain_stats_data
from process.data.utils import stats_data_proc
from pandas import DataFrame as pdDataFrame
from sklearn.preprocessing import LabelEncoder


def obtain_data(cfg: dict, api_key: str):
    """
    Obtains and processes population statistics data based on the provided configuration and API key.

    The function performs the following steps:
    1. Fetches raw statistics data using the API configuration and key.
    2. Processes the raw data according to the configuration.
    3. Applies inclusion and exclusion filters to the data based on specified criteria.
    4. Maps the filtered data to the desired columns.
    5. Expands the DataFrame by repeating rows according to the 'value' column.
    6. Returns the final DataFrame with the 'value' column removed and index reset.

    Args:
        cfg (dict): Configuration dictionary containing API settings, mapping of column names,
            inclusion and exclusion criteria.
        api_key (str): API key for accessing the statistics data.

    Returns:
        pandas.DataFrame: Processed and filtered DataFrame with population statistics.
    """

    def _obtain_qc_key(cfg, proc_qc_type):
        if proc_qc_type in cfg["map"]:
            return cfg["map"][proc_qc_type]
        return proc_qc_type

    data_pop = obtain_stats_data(cfg["api"], api_key=api_key)
    data_pop = stats_data_proc(data_pop, cfg)

    try:
        for proc_qc_type in cfg["inclusion"]:
            proc_qc_key = _obtain_qc_key(cfg, proc_qc_type)
            data_pop = data_pop[
                data_pop[proc_qc_key].isin(cfg["inclusion"][proc_qc_type])
            ]
    except TypeError:
        pass

    try:
        for proc_qc_type in cfg["exclusion"]:
            proc_qc_key = _obtain_qc_key(cfg, proc_qc_type)
            data_pop = data_pop[
                ~data_pop[proc_qc_key].isin(cfg["exclusion"][proc_qc_type])
            ]
    except TypeError:
        pass

    df = data_pop[list(cfg["map"].values())]

    return df


def encode_weights(data_dict) -> dict:

    for key in data_dict:

        run_repeat = False
        if key == "seed":
            run_repeat = True

        df = data_dict[key]

        if run_repeat:
            df = df.loc[df.index.repeat(df["value"])].copy()
            df = df.reset_index(drop=True).drop(columns=["value"])
        else:
            group_cols = df.columns.drop("value").tolist()
            df_grouped = df.groupby(group_cols, as_index=False)["value"].sum()
            df_grouped["probability"] = df_grouped["value"] / df_grouped["value"].sum()
            df = df_grouped.reset_index(drop=True).drop(columns=["value"])

        data_dict[key] = df

    return data_dict
