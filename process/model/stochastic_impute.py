import pandas as pd
import numpy as np
from process.data.data import encode_weights
from os.path import exists as os_path_exists
from os import makedirs as os_makedirs
from process.model.utils import check_deps_charts


def get_target_values(group, prob_mapping):
    """
    Helper function to generate target values.
    Returns a Series to be assigned directly to the dataframe.
    """
    keys = group.name if isinstance(group.name, tuple) else (group.name,)

    if keys in prob_mapping:
        target_vals, w = prob_mapping[keys]
        return pd.Series(
            np.random.choice(target_vals, size=len(group), p=w), index=group.index
        )
    else:
        return pd.Series(np.nan, index=group.index)


def stochastic_impute(
    data_dict,
    task_list,
    output_dir="./output",
    output_filename="stochastic_imputed_data.parquet",
):

    data_dict = encode_weights(data_dict)

    result_df = data_dict["seed"].copy()

    for proc_task in task_list:

        proc_task = proc_task.strip()
        proc_targets = task_list[proc_task]["targets"]
        check_data = data_dict[proc_task].copy()

        shared_cols = list(set(result_df.columns).intersection(check_data.columns))
        # Find all unique patterns of missing data in the shared columns
        # This creates a boolean dataframe of True (is NaN) / False (is valid)
        null_mask = result_df[shared_cols].isna()
        unique_patterns = null_mask.drop_duplicates()

        if not shared_cols:
            continue
        for proc_target in proc_targets:

            # 1. Create an empty Series to hold the results for this task
            new_col = pd.Series(index=result_df.index, dtype=object)

            # 2. Iterate through each distinct missingness pattern
            for _, pattern in unique_patterns.iterrows():
                # Get only the columns that are VALID (Not NaN) for this pattern
                valid_cols = [col for col in shared_cols if not pattern[col]]

                try:
                    valid_cols.remove(proc_target)
                except ValueError:
                    pass

                # Filter result_df for rows matching this exact missingness pattern
                row_mask = (null_mask[shared_cols] == pattern).all(axis=1)
                subset_df = result_df[row_mask]

                # --- CASE A: All shared columns are NaN ---
                if len(valid_cols) == 0:
                    raise ValueError(
                        f"All shared columns are NaN for task '{proc_target}'. Cannot impute without any valid columns."
                    )

                # --- CASE B: At least one valid column exists ---
                # Aggregate check_data using ONLY the valid columns for this specific chunk
                check_data_agg = check_data.groupby(
                    valid_cols + [proc_target], as_index=False
                )["probability"].sum()

                # Build the probability mapping
                prob_mapping = {}
                for keys, group in check_data_agg.groupby(valid_cols):
                    t_vals = group[proc_target].values
                    w = group["probability"].values
                    w = w / w.sum() if w.sum() > 0 else np.ones(len(w)) / len(w)

                    keys = keys if isinstance(keys, tuple) else (keys,)
                    prob_mapping[keys] = (t_vals, w)

                # Apply the mapping to this specific subset
                assigned_subset = subset_df.groupby(valid_cols, group_keys=False).apply(
                    get_target_values, prob_mapping=prob_mapping, include_groups=False
                )

                # Update our master column with the results from this chunk
                new_col.update(assigned_subset)

            # 4. Attach the fully processed column back to the dataframe (WITH MEAN LOGIC)
            if proc_target in result_df.columns:
                # Combine the existing column and the new column, then take the row-wise mean
                combined_df = pd.DataFrame(
                    {"existing": result_df[proc_target], "new": new_col}
                )

                if proc_targets[proc_target] != "category":
                    result_df[proc_target] = combined_df.mean(axis=1, skipna=True)
                else:
                    result_df[proc_target] = combined_df.apply(
                        lambda row: (
                            np.random.choice(row.dropna().values)
                            if row.notna().any()
                            else np.nan
                        ),
                        axis=1,
                    )
            else:
                # If it doesn't exist yet, just assign the new column directly
                result_df[proc_target] = new_col

    if output_dir is not None:

        if not os_path_exists(output_dir):
            os_makedirs(output_dir)
        check_deps_charts(task_list, output_dir=output_dir)

        result_df.to_parquet(f"{output_dir}/{output_filename}")

    return result_df
