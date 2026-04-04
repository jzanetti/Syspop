from pandas import to_numeric
from pandas import DataFrame


def stats_data_proc(data: DataFrame, cfg: dict):

    data = data.rename(columns=cfg["map"])

    data["value"] = to_numeric(data["value"], errors="coerce")
    data = data.dropna()
    data["value"] = data["value"].astype(int)

    return data


def check_data_consistency(
    data_dict: dict,
    check_err: bool = True,
    throw_err: bool = False,
    output_dir: str or None = None,
):
    rows = []

    for key, df in data_dict.items():
        for col in df.columns:
            unique_list = df[col].unique()
            unique_str = ", ".join(map(str, unique_list))
            unique_str = [item.strip() for item in unique_str.split(",")]
            unique_str.sort()
            unique_str = ", ".join(unique_str)

            unique_count = len(unique_list)
            rows.append(
                {
                    "data_key": key,
                    "cols": col,
                    "unique_count": unique_count,
                    "unique_value": unique_str,
                }
            )

    summary_df = DataFrame(rows)
    if output_dir is not None:
        summary_df.to_csv(f"{output_dir}/data_consistency.csv", index=False)

    if check_err:
        all_cols = summary_df["cols"].unique()

        for proc_col in all_cols:
            proc_series = summary_df[summary_df["cols"] == proc_col]["unique_value"]

            proc_series = [set(val.split(", ")) for val in proc_series]

            all_same = all(s == proc_series[0] for s in proc_series)

            if not all_same:
                print(
                    summary_df[summary_df["cols"] == proc_col][
                        ["data_key", "cols", "unique_value"]
                    ]
                )

                if throw_err:
                    raise ValueError(
                        f"Column '{proc_col}' has different unique values across datasets."
                    )

                # Ask the user if they want to continue
                user_choice = (
                    input(
                        f"\nColumn '{proc_col}' has different unique values "
                        + "across datasets. Continue? [Y/N]: "
                    )
                    .strip()
                    .upper()
                )
                if user_choice != "Y":
                    raise ValueError(
                        f"Execution halted by user. Column '{proc_col}' has "
                        + "different unique values across datasets."
                    )
