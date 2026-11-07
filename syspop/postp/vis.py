from itertools import combinations
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def _is_categorical(series: pd.Series, max_unique: int = 15) -> bool:
    return (
        series.dtype == "object"
        or str(series.dtype) == "category"
        or series.dtype == bool
        or series.nunique(dropna=True) <= max_unique
    )


def plot_distribution(df, columns, output_dir="./output", dropna=True):
    """
    Plots the distribution of 1 or 2 columns from a DataFrame and saves it as a PNG
    using pure matplotlib.
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

        if _is_categorical(plot_df[col]):
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
        cat1 = _is_categorical(plot_df[col1])
        cat2 = _is_categorical(plot_df[col2])
        fig, ax = plt.subplots(figsize=(10, 6))

        if cat1 and cat2:
            crosstab = pd.crosstab(plot_df[col1], plot_df[col2])
            im = ax.imshow(crosstab.values, cmap="YlGnBu", aspect="auto")
            fig.colorbar(im, ax=ax, label="Count")
            ax.set_xticks(np.arange(len(crosstab.columns)))
            ax.set_yticks(np.arange(len(crosstab.index)))
            ax.set_xticklabels([str(c) for c in crosstab.columns], rotation=45, ha="right")
            ax.set_yticklabels([str(r) for r in crosstab.index])
            ax.set_xlabel(col2)
            ax.set_ylabel(col1)
            ax.set_title(f"Cross Distribution: {col1} vs {col2}", fontsize=14)
        elif cat1 != cat2:
            cat_col = col1 if cat1 else col2
            cont_col = col2 if cat1 else col1
            cats = sorted(plot_df[cat_col].dropna().unique(), key=str)
            groups = [
                pd.to_numeric(plot_df.loc[plot_df[cat_col] == c, cont_col], errors="coerce").dropna().to_numpy()
                for c in cats
            ]
            ax.boxplot(groups, tick_labels=[str(c) for c in cats], patch_artist=True)
            ax.set_xlabel(cat_col)
            ax.set_ylabel(cont_col)
            ax.set_title(f"{cont_col} by {cat_col}", fontsize=14)
            plt.xticks(rotation=35, ha="right")
        else:
            x_vals = pd.to_numeric(plot_df[col1], errors="coerce").to_numpy()
            y_vals = pd.to_numeric(plot_df[col2], errors="coerce").to_numpy()
            ax.scatter(x_vals, y_vals, alpha=0.5, color="#2b7bba", edgecolors="none")
            ax.set_xlabel(col1)
            ax.set_ylabel(col2)
            ax.set_title(f"{col1} vs {col2}", fontsize=14)

        plt.tight_layout()

    plt.savefig(output_filename)
    plt.close()
    return output_filename


def plot_comparison(
    raw_df: pd.DataFrame,
    syn_df: pd.DataFrame,
    columns: list,
    output_dir: str = "./output",
    filename: str = "comparison_raw_vs_synthetic.png",
):
    """
    Plot side-by-side 1D comparison of feature distributions between the original
    unit-record dataset (`raw_df`) and the recreated synthetic dataset (`syn_df`).
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
        if _is_categorical(raw_df[col]):
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


def _plot_single_pair_comparison(
    raw_df: pd.DataFrame,
    syn_df: pd.DataFrame,
    col1: str,
    col2: str,
    output_dir: str,
) -> str:
    """
    Create a side-by-side Raw vs. Synthetic comparison figure for a pair of features `(col1, col2)`:
      - Categorical x Categorical: Raw heatmap, Synthetic heatmap, and grouped bar comparison.
      - Categorical x Continuous : Mean +/- SD bar comparison and side-by-side boxplot per category.
      - Continuous  x Continuous : Overlay scatter + binned trend line and 2D density histograms.
    """
    cat1 = _is_categorical(raw_df[col1])
    cat2 = _is_categorical(raw_df[col2])
    out_path = os.path.join(output_dir, f"comparison_{col1}_{col2}.png")

    if cat1 and cat2:
        cats1 = sorted(set(raw_df[col1].dropna()).union(set(syn_df[col1].dropna())), key=str)
        cats2 = sorted(set(raw_df[col2].dropna()).union(set(syn_df[col2].dropna())), key=str)

        raw_ct = (
            pd.crosstab(raw_df[col1], raw_df[col2], normalize="all")
            .reindex(index=cats1, columns=cats2, fill_value=0.0)
        )
        syn_ct = (
            pd.crosstab(syn_df[col1], syn_df[col2], normalize="all")
            .reindex(index=cats1, columns=cats2, fill_value=0.0)
        )
        vmax = max(float(raw_ct.to_numpy().max()), float(syn_ct.to_numpy().max()), 0.01)

        fig, axes = plt.subplots(1, 3, figsize=(18, 5.5))

        for ax, ct, title in zip(
            axes[:2],
            [raw_ct, syn_ct],
            [f"Raw: {col1} vs {col2} (%)", f"Synthetic: {col1} vs {col2} (%)"],
        ):
            im = ax.imshow(ct.values * 100.0, cmap="YlGnBu", vmin=0.0, vmax=vmax * 100.0, aspect="auto")
            fig.colorbar(im, ax=ax, label="Share of Total (%)")
            ax.set_xticks(np.arange(len(cats2)))
            ax.set_yticks(np.arange(len(cats1)))
            ax.set_xticklabels([str(c) for c in cats2], rotation=35, ha="right")
            ax.set_yticklabels([str(r) for r in cats1])
            ax.set_xlabel(col2)
            ax.set_ylabel(col1)
            ax.set_title(title)
            for i in range(len(cats1)):
                for j in range(len(cats2)):
                    val = ct.values[i, j] * 100.0
                    text_color = "white" if val > (vmax * 55.0) else "black"
                    ax.text(j, i, f"{val:.1f}%", ha="center", va="center", color=text_color, fontsize=9)

        # Panel 3: Grouped bar chart of all (col1, col2) cells
        ax3 = axes[2]
        pair_labels = [f"{r}\n{c}" for r in cats1 for c in cats2]
        raw_flat = (raw_ct.values.flatten()) * 100.0
        syn_flat = (syn_ct.values.flatten()) * 100.0
        x = np.arange(len(pair_labels))
        width = 0.38
        ax3.bar(x - width / 2, raw_flat, width, label="Raw", color="#2b7bba", alpha=0.85)
        ax3.bar(x + width / 2, syn_flat, width, label="Synthetic", color="#e66101", alpha=0.85)
        ax3.set_xticks(x)
        ax3.set_xticklabels(pair_labels, rotation=45, ha="right", fontsize=8)
        ax3.set_ylabel("Share of Total (%)")
        ax3.set_title(f"Cell Proportions: {col1} × {col2}")
        ax3.legend()

        plt.tight_layout()
        plt.savefig(out_path)
        plt.close()
        return out_path

    elif cat1 != cat2:
        cat_col = col1 if cat1 else col2
        cont_col = col2 if cat1 else col1
        cats = sorted(set(raw_df[cat_col].dropna()).union(set(syn_df[cat_col].dropna())), key=str)

        fig, axes = plt.subplots(1, 2, figsize=(15, 5.5))

        # Panel 1: Mean +/- SD per category (Raw vs Synthetic)
        ax1 = axes[0]
        raw_means, raw_stds = [], []
        syn_means, syn_stds = [], []
        raw_groups, syn_groups = [], []

        for c in cats:
            r_s = pd.to_numeric(raw_df.loc[raw_df[cat_col] == c, cont_col], errors="coerce").dropna().to_numpy()
            s_s = pd.to_numeric(syn_df.loc[syn_df[cat_col] == c, cont_col], errors="coerce").dropna().to_numpy()
            raw_groups.append(r_s if len(r_s) > 0 else np.array([0.0]))
            syn_groups.append(s_s if len(s_s) > 0 else np.array([0.0]))
            raw_means.append(float(np.mean(r_s)) if len(r_s) > 0 else 0.0)
            raw_stds.append(float(np.std(r_s, ddof=1)) if len(r_s) > 1 else 0.0)
            syn_means.append(float(np.mean(s_s)) if len(s_s) > 0 else 0.0)
            syn_stds.append(float(np.std(s_s, ddof=1)) if len(s_s) > 1 else 0.0)

        x = np.arange(len(cats))
        width = 0.36
        ax1.bar(
            x - width / 2,
            raw_means,
            width,
            yerr=raw_stds,
            capsize=4,
            label="Raw (Mean ± SD)",
            color="#2b7bba",
            alpha=0.85,
        )
        ax1.bar(
            x + width / 2,
            syn_means,
            width,
            yerr=syn_stds,
            capsize=4,
            label="Synthetic (Mean ± SD)",
            color="#e66101",
            alpha=0.85,
        )
        ax1.set_xticks(x)
        ax1.set_xticklabels([str(c) for c in cats], rotation=30, ha="right")
        ax1.set_xlabel(cat_col)
        ax1.set_ylabel(cont_col)
        ax1.set_title(f"{cont_col} by {cat_col} (Mean ± 1 SD)")
        ax1.legend()

        # Panel 2: Side-by-side Boxplots per category
        ax2 = axes[1]
        pos_raw = x - 0.18
        pos_syn = x + 0.18
        bp_raw = ax2.boxplot(
            raw_groups,
            positions=pos_raw,
            widths=0.3,
            patch_artist=True,
            showfliers=False,
        )
        bp_syn = ax2.boxplot(
            syn_groups,
            positions=pos_syn,
            widths=0.3,
            patch_artist=True,
            showfliers=False,
        )
        for patch in bp_raw["boxes"]:
            patch.set_facecolor("#2b7bba")
            patch.set_alpha(0.75)
        for patch in bp_syn["boxes"]:
            patch.set_facecolor("#e66101")
            patch.set_alpha(0.75)

        ax2.set_xticks(x)
        ax2.set_xticklabels([str(c) for c in cats], rotation=30, ha="right")
        ax2.set_xlabel(cat_col)
        ax2.set_ylabel(cont_col)
        ax2.set_title(f"{cont_col} Distribution by {cat_col} (Median & IQR)")
        ax2.plot([], [], color="#2b7bba", linewidth=6, label="Raw")
        ax2.plot([], [], color="#e66101", linewidth=6, label="Synthetic")
        ax2.legend()

        plt.tight_layout()
        plt.savefig(out_path)
        plt.close()
        return out_path

    else:
        # Both continuous
        rx = pd.to_numeric(raw_df[col1], errors="coerce").to_numpy()
        ry = pd.to_numeric(raw_df[col2], errors="coerce").to_numpy()
        sx = pd.to_numeric(syn_df[col1], errors="coerce").to_numpy()
        sy = pd.to_numeric(syn_df[col2], errors="coerce").to_numpy()

        rmask = ~np.isnan(rx) & ~np.isnan(ry)
        smask = ~np.isnan(sx) & ~np.isnan(sy)
        rx, ry = rx[rmask], ry[rmask]
        sx, sy = sx[smask], sy[smask]

        fig, axes = plt.subplots(1, 3, figsize=(17, 5))

        # Panel 1: Scatter overlay
        ax1 = axes[0]
        ax1.scatter(rx, ry, alpha=0.35, s=18, color="#2b7bba", label="Raw")
        ax1.scatter(sx, sy, alpha=0.35, s=18, color="#e66101", label="Synthetic")
        ax1.set_xlabel(col1)
        ax1.set_ylabel(col2)
        ax1.set_title(f"Scatter: {col1} vs {col2}")
        ax1.legend()

        # Panel 2 & 3: 2D joint histograms on shared bin edges
        x_min, x_max = float(min(rx.min(), sx.min())), float(max(rx.max(), sx.max()))
        y_min, y_max = float(min(ry.min(), sy.min())), float(max(ry.max(), sy.max()))
        x_bins = np.linspace(x_min, x_max, 18)
        y_bins = np.linspace(y_min, y_max, 18)

        for ax, x_arr, y_arr, title in zip(
            axes[1:],
            [rx, sx],
            [ry, sy],
            [f"Raw 2D Density: {col1} vs {col2}", f"Synthetic 2D Density: {col1} vs {col2}"],
        ):
            h = ax.hist2d(x_arr, y_arr, bins=[x_bins, y_bins], cmap="YlGnBu", density=True)
            fig.colorbar(h[3], ax=ax, label="Density")
            ax.set_xlabel(col1)
            ax.set_ylabel(col2)
            ax.set_title(title)

        plt.tight_layout()
        plt.savefig(out_path)
        plt.close()
        return out_path


def plot_cross_comparison(
    raw_df: pd.DataFrame,
    syn_df: pd.DataFrame,
    columns: list,
    output_dir: str = "./output",
    focus_column: str or None = None,
) -> list:
    """
    Generate Raw vs. Synthetic cross-feature comparison plots (`comparison_{col1}_{col2}.png`).

    Parameters:
      - raw_df: Confidential unit-record DataFrame.
      - syn_df: Recreated synthetic unit-record DataFrame.
      - columns: List of feature names to compare.
      - output_dir: Directory where PNG plots will be saved.
      - focus_column: Optional column name (e.g. 'occupation'). If provided, generates
        comparisons for `(focus_column, other_col)` for every other column in `columns`.
        If None (default), generates comparisons for all pairwise combinations of `columns`.

    Returns:
      - List of saved PNG file paths.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if focus_column is not None:
        pairs = [(focus_column, c) for c in columns if c != focus_column]
    else:
        pairs = list(combinations(columns, 2))

    saved_paths = []
    for col1, col2 in pairs:
        path = _plot_single_pair_comparison(raw_df, syn_df, col1, col2, output_dir)
        saved_paths.append(path)

    return saved_paths
