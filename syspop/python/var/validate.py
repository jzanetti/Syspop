from pandas import DataFrame as pandas_dataframe
from pandas import merge as pandas_merge


def validate_num(
    analysis: pandas_dataframe,
    truth: pandas_dataframe,
    ref_columns: list = ["age", "ethnicity"],
) -> pandas_dataframe:
    """Validate num between analysis and truth

    Args:
        analysis (pandas_dataframe): Analysis field
        truth (pandas_dataframe): Truth dataset
        ref_columns (list, optional): reference columns. Defaults to ["age", "ethnicity"].
    """
    return pandas_merge(
        analysis.groupby(ref_columns)["num"].sum().reset_index(),
        truth,
        how="right",
        on=ref_columns,
        suffixes=("_analysis", "_truth"),
    )


def validate_percentage(
    analysis: pandas_dataframe,
    truth: pandas_dataframe,
    percentage_field_name: str,
    ref_columns: list = ["age", "ethnicity"],
) -> pandas_dataframe:
    # control_columns = list(input_data[proc_key]["data"].columns)
    proc_analysis = analysis[ref_columns + ["num"]]
    proc_analysis["num"] = (
        proc_analysis.groupby(ref_columns)["num"].transform("sum").drop_duplicates()
    )
    groupby_columns = [
        col for col in ["age", "ethnicity", "depression"] if col in ref_columns
    ]

    proc_analysis["num"] = proc_analysis.groupby(groupby_columns)["num"].transform(
        lambda x: x / x.sum()
    )

    return pandas_merge(
        proc_analysis,
        truth,
        how="right",
        on=groupby_columns + [percentage_field_name],
        suffixes=("_analysis", f"_truth"),
    ).dropna()
