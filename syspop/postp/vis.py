import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def plot_distribution(df, columns, output_dir="./output", dropna=True):
    """
    Plots the distribution of 1 or 2 columns from a DataFrame and saves it as a PNG
    using pure matplotlib (no extra plotting libraries required).

    Parameters:
    - df: The pandas DataFrame.
    - columns: A single string (column name) or a list of 1 or 2 column names.
    - dropna: Boolean, whether to drop NaN values before plotting.
    """
    if isinstance(columns, str):
        columns = [columns]

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    plot_df = df.dropna(subset=columns) if dropna else df.copy()
    output_filename = os.path.join(output_dir, f"distribution_{'_'.join(columns)}.png")

    if len(columns) == 1:
        col = columns[0]
        fig, ax = plt.subplots(figsize=(10, 6))

        if plot_df[col].dtype == "object" or plot_df[col].nunique() < 20:
            counts = plot_df[col].value_counts().sort_index()
            x_labels = [str(k) for k in counts.index]
            ax.bar(x_labels, counts.values, color="#2b7bba", edgecolor="black", alpha=0.85)
            ax.set_title(f"Distribution of {col}", fontsize=14)
            ax.set_xlabel(col)
            ax.set_ylabel("Count")
            plt.xticks(rotation=45, ha="right")
        else:
            vals = pd.to_numeric(plot_df[col], errors="coerce").dropna().to_numpy()
            ax.hist(vals, bins=30, color="#2b7bba", edgecolor="black", alpha=0.75)
            ax.set_title(f"Distribution of {col}", fontsize=14)
            ax.set_xlabel(col)
            ax.set_ylabel("Frequency")

        plt.tight_layout()

    elif len(columns) == 2:
        col1, col2 = columns
        fig, ax = plt.subplots(figsize=(12, 8))

        crosstab = pd.crosstab(plot_df[col1], plot_df[col2])
        im = ax.imshow(crosstab.values, cmap="YlGnBu", aspect="auto")
        fig.colorbar(im, ax=ax, label="Count")

        ax.set_xticks(np.arange(len(crosstab.columns)))
        ax.set_yticks(np.arange(len(crosstab.index)))
        ax.set_xticklabels([str(c) for c in crosstab.columns], rotation=45, ha="right")
        ax.set_yticklabels([str(r) for r in crosstab.index])
        ax.set_xlabel(col2)
        ax.set_ylabel(col1)
        ax.set_title(f"Heatmap of {col1} vs {col2}", fontsize=14)

        plt.tight_layout()

    plt.savefig(output_filename)
    plt.close()


def plot_comparison(
    raw_df: pd.DataFrame,
    syn_df: pd.DataFrame,
    columns: list,
    output_dir: str = "./output",
    filename: str = "comparison_raw_vs_synthetic.png",
):
    """
    Plot side-by-side comparison of feature distributions between the original
    unit-record dataset (`raw_df`) and the recreated synthetic dataset (`syn_df`)
    using pure matplotlib.
    """
    if isinstance(columns, str):
        columns = [columns]

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    n_cols = len(columns)
    n_grid_cols = min(3, n_cols)
    n_grid_rows = (n_cols + n_grid_cols - 1) // n_grid_cols

    fig, axes = plt.subplots(
        n_grid_rows, n_grid_cols, figsize=(6 * n_grid_cols, 4.5 * n_grid_rows)
    )
    if n_cols == 1:
        axes = [axes]
    else:
        axes = np.asarray(axes).flatten()

    for idx, col in enumerate(columns):
        ax = axes[idx]
        if raw_df[col].dtype == "object" or raw_df[col].nunique() < 15:
            raw_props = raw_df[col].value_counts(normalize=True)
            syn_props = syn_df[col].value_counts(normalize=True)
            cats = sorted(set(raw_props.index).union(set(syn_props.index)), key=str)
            x = np.arange(len(cats))
            width = 0.38
            r_vals = [float(raw_props.get(c, 0.0)) for c in cats]
            s_vals = [float(syn_props.get(c, 0.0)) for c in cats]
            ax.bar(x - width / 2, r_vals, width, label="Raw", color="#2b7bba", alpha=0.85)
            ax.bar(x + width / 2, s_vals, width, label="Synthetic", color="#e66101", alpha=0.85)
            ax.set_xticks(x)
            ax.set_xticklabels([str(c) for c in cats], rotation=35, ha="right")
            ax.set_ylabel("Proportion")
        else:
            r_vals = pd.to_numeric(raw_df[col], errors="coerce").dropna().to_numpy()
            s_vals = pd.to_numeric(syn_df[col], errors="coerce").dropna().to_numpy()
            combined = np.concatenate([r_vals, s_vals])
            bins = np.linspace(np.min(combined), np.max(combined), 25)
            ax.hist(r_vals, bins=bins, density=True, alpha=0.55, label="Raw", color="#2b7bba", edgecolor="black")
            ax.hist(s_vals, bins=bins, density=True, alpha=0.55, label="Synthetic", color="#e66101", edgecolor="black")
            ax.set_ylabel("Density")

        ax.set_title(f"{col}: Raw vs. Synthetic")
        ax.legend()

    for extra_idx in range(n_cols, len(axes)):
        axes[extra_idx].axis("off")

    plt.tight_layout()
    out_path = os.path.join(output_dir, filename)
    plt.savefig(out_path)
    plt.close()
    return out_path
