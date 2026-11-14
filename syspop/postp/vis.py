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


def compute_idi_safe_1d_bins(
    r_vals: np.ndarray,
    s_vals: np.ndarray,
    min_bin_count: int = 20,
    max_bins: int = 20,
) -> np.ndarray:
    """
    Compute non-uniform 1D histogram bin edges covering both `r_vals` and `s_vals`
    such that every bin contains at least `min_bin_count` raw records (when
    `len(r_vals) >= min_bin_count`), satisfying Stats NZ IDI output guidance.
    """
    all_min = float(min(np.min(r_vals), np.min(s_vals)))
    all_max = float(max(np.max(r_vals), np.max(s_vals)))
    if all_min == all_max:
        return np.array([all_min - 0.5, all_max + 0.5], dtype=float)

    n_target = min(max_bins, max(1, len(r_vals) // max(int(min_bin_count), 1)))
    if n_target <= 1 or np.all(r_vals == r_vals[0]):
        return np.array([all_min, all_max], dtype=float)

    edges = np.unique(np.quantile(r_vals, np.linspace(0.0, 1.0, n_target + 1)))
    edges[0] = min(float(edges[0]), all_min)
    edges[-1] = max(float(edges[-1]), all_max)
    if len(edges) <= 2:
        return np.array([all_min, all_max], dtype=float)

    # Greedily merge any bin with raw_count < min_bin_count with its smaller neighbour
    edges_list = list(edges.astype(float))
    while len(edges_list) > 2:
        cnts, _ = np.histogram(r_vals, bins=edges_list)
        small_idx = [i for i, c in enumerate(cnts) if c < min_bin_count]
        if not small_idx:
            break
        i = small_idx[0]
        if i == 0:
            del edges_list[1]
        elif i == len(cnts) - 1:
            del edges_list[-2]
        elif cnts[i - 1] <= cnts[i + 1]:
            del edges_list[i]
        else:
            del edges_list[i + 1]

    return np.array(edges_list, dtype=float)


def compute_idi_safe_2d_bins(
    rx: np.ndarray,
    ry: np.ndarray,
    sx: np.ndarray,
    sy: np.ndarray,
    min_bin_count: int = 6,
    max_bins: int = 8,
) -> tuple:
    """
    Compute non-uniform quantile-based 2D bin edges `(x_bins, y_bins)` covering both
    raw and synthetic points for Stats NZ IDI-compliant 2D density plots.
    """
    x_min = float(min(np.min(rx), np.min(sx)))
    x_max = float(max(np.max(rx), np.max(sx)))
    y_min = float(min(np.min(ry), np.min(sy)))
    y_max = float(max(np.max(ry), np.max(sy)))

    k = min(max_bins, max(2, int(np.sqrt(len(rx) / max(int(min_bin_count), 1)))))

    def _axis_edges(r_arr, a_min, a_max):
        if a_min == a_max:
            return np.array([a_min - 0.5, a_max + 0.5], dtype=float)
        edges = np.unique(np.quantile(r_arr, np.linspace(0.0, 1.0, k + 1)))
        edges[0] = min(float(edges[0]), a_min)
        edges[-1] = max(float(edges[-1]), a_max)
        if len(edges) < 2:
            return np.array([a_min, a_max], dtype=float)
        return edges.astype(float)

    return _axis_edges(rx, x_min, x_max), _axis_edges(ry, y_min, y_max)


def plot_comparison(
    raw_df: pd.DataFrame,
    syn_df: pd.DataFrame,
    columns: list,
    output_dir: str = "./output",
    filename: str = "comparison_raw_vs_synthetic.png",
    min_count: int or None = None,
    config_or_path=None,
):
    """
    Plot side-by-side 1D comparison of feature distributions between the original
    unit-record dataset (`raw_df`) and the recreated synthetic dataset (`syn_df`).
    """
    if min_count is None:
        if config_or_path is not None:
            from syspop.confidential.config import resolve_verification_config

            min_count = resolve_verification_config(config_or_path)["min_count"]
        else:
            min_count = 6

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
            raw_counts = raw_df[col].value_counts()
            raw_props = raw_df[col].value_counts(normalize=True)
            syn_props = syn_df[col].value_counts(normalize=True)
            cats = sorted(set(raw_props.index).union(set(syn_props.index)), key=str)
            x = np.arange(len(cats))
            width = 0.38
            r_vals = [
                np.nan if 0 < int(raw_counts.get(c, 0)) < min_count else float(raw_props.get(c, 0.0))
                for c in cats
            ]
            s_vals = [float(syn_props.get(c, 0.0)) for c in cats]
            ax.bar(x - width / 2, r_vals, width, label="Raw", color="#2b7bba", alpha=0.85)
            ax.bar(x + width / 2, s_vals, width, label="Synthetic", color="#e66101", alpha=0.85)
            for xi, c in enumerate(cats):
                if 0 < int(raw_counts.get(c, 0)) < min_count:
                    ax.text(xi - width / 2, 0.01, "S", ha="center", va="bottom", fontsize=8, color="#555555")
            ax.set_xticks(x)
            ax.set_xticklabels([str(c) for c in cats], rotation=35, ha="right")
            ax.set_ylabel("Proportion")
        else:
            r_vals = pd.to_numeric(raw_df[col], errors="coerce").dropna().to_numpy()
            s_vals = pd.to_numeric(syn_df[col], errors="coerce").dropna().to_numpy()
            bins = compute_idi_safe_1d_bins(r_vals, s_vals, min_bin_count=min_count)
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
    include_scatter: bool = False,
    min_count: int = 6,
) -> str:
    """
    Create a side-by-side Raw vs. Synthetic comparison figure for a pair of features `(col1, col2)`:
      - Categorical x Categorical: Raw heatmap, Synthetic heatmap, and grouped bar comparison.
      - Categorical x Continuous : Mean +/- SD bar comparison and side-by-side boxplot per category.
      - Continuous  x Continuous : 2D density histograms (plus optional scatter when `include_scatter=True`).
    """
    cat1 = _is_categorical(raw_df[col1])
    cat2 = _is_categorical(raw_df[col2])
    out_path = os.path.join(output_dir, f"comparison_{col1}_{col2}.png")

    if cat1 and cat2:
        cats1 = sorted(set(raw_df[col1].dropna()).union(set(syn_df[col1].dropna())), key=str)
        cats2 = sorted(set(raw_df[col2].dropna()).union(set(syn_df[col2].dropna())), key=str)

        raw_counts = (
            pd.crosstab(raw_df[col1], raw_df[col2])
            .reindex(index=cats1, columns=cats2, fill_value=0)
        )
        raw_ct = (
            pd.crosstab(raw_df[col1], raw_df[col2], normalize="all")
            .reindex(index=cats1, columns=cats2, fill_value=0.0)
        )
        syn_ct = (
            pd.crosstab(syn_df[col1], syn_df[col2], normalize="all")
            .reindex(index=cats1, columns=cats2, fill_value=0.0)
        )
        suppressed_mask = (raw_counts.values > 0) & (raw_counts.values < min_count)
        raw_vals_pct = np.where(suppressed_mask, np.nan, raw_ct.values * 100.0)
        syn_vals_pct = syn_ct.values * 100.0
        raw_max = float(np.nanmax(raw_vals_pct)) if np.any(~np.isnan(raw_vals_pct)) else 0.0
        vmax = max(raw_max, float(syn_vals_pct.max()), 1.0)

        fig, axes = plt.subplots(1, 3, figsize=(18, 5.5))
        cmap = plt.get_cmap("YlGnBu").copy()
        cmap.set_bad(color="#e0e0e0")

        for ax, mat, is_raw, title in zip(
            axes[:2],
            [raw_vals_pct, syn_vals_pct],
            [True, False],
            [f"Raw: {col1} vs {col2} (%)", f"Synthetic: {col1} vs {col2} (%)"],
        ):
            im = ax.imshow(mat, cmap=cmap, vmin=0.0, vmax=vmax, aspect="auto")
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
                    if is_raw and suppressed_mask[i, j]:
                        ax.text(j, i, "S", ha="center", va="center", color="#444444", fontsize=9, fontweight="bold")
                    else:
                        val = mat[i, j]
                        text_color = "white" if val > (vmax * 0.55) else "black"
                        ax.text(j, i, f"{val:.1f}%", ha="center", va="center", color=text_color, fontsize=9)

        # Panel 3: Grouped bar chart of all (col1, col2) cells
        ax3 = axes[2]
        pair_labels = [f"{r}\n{c}" for r in cats1 for c in cats2]
        raw_flat = raw_vals_pct.flatten()
        syn_flat = syn_vals_pct.flatten()
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
        raw_groups, raw_positions = [], []
        syn_groups, syn_positions = [], []

        for idx_c, c in enumerate(cats):
            r_s = pd.to_numeric(raw_df.loc[raw_df[cat_col] == c, cont_col], errors="coerce").dropna().to_numpy()
            s_s = pd.to_numeric(syn_df.loc[syn_df[cat_col] == c, cont_col], errors="coerce").dropna().to_numpy()
            if len(r_s) >= min_count:
                raw_groups.append(r_s)
                raw_positions.append(idx_c - 0.18)
                raw_means.append(float(np.mean(r_s)))
                raw_stds.append(float(np.std(r_s, ddof=1)) if len(r_s) > 1 else 0.0)
            else:
                raw_means.append(np.nan)
                raw_stds.append(0.0)
            if len(s_s) > 0:
                syn_groups.append(s_s)
                syn_positions.append(idx_c + 0.18)
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
        for idx_c, rm in enumerate(raw_means):
            if np.isnan(rm):
                ax1.text(idx_c - width / 2, 0.0, "S", ha="center", va="bottom", fontsize=8, color="#555555")
        ax1.set_xticks(x)
        ax1.set_xticklabels([str(c) for c in cats], rotation=30, ha="right")
        ax1.set_xlabel(cat_col)
        ax1.set_ylabel(cont_col)
        ax1.set_title(f"{cont_col} by {cat_col} (Mean ± 1 SD)")
        ax1.legend()

        # Panel 2: Side-by-side Boxplots per category
        ax2 = axes[1]
        if raw_groups:
            bp_raw = ax2.boxplot(
                raw_groups,
                positions=raw_positions,
                widths=0.3,
                patch_artist=True,
                showfliers=False,
            )
            for patch in bp_raw["boxes"]:
                patch.set_facecolor("#2b7bba")
                patch.set_alpha(0.75)
        if syn_groups:
            bp_syn = ax2.boxplot(
                syn_groups,
                positions=syn_positions,
                widths=0.3,
                patch_artist=True,
                showfliers=False,
            )
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

        # 2D joint density on adaptive non-uniform quantile bin edges with IDI cell suppression
        x_bins, y_bins = compute_idi_safe_2d_bins(rx, ry, sx, sy, min_bin_count=min_count)

        h_raw_cnt, _, _ = np.histogram2d(rx, ry, bins=[x_bins, y_bins])
        h_raw_den, _, _ = np.histogram2d(rx, ry, bins=[x_bins, y_bins], density=True)
        h_syn_den, _, _ = np.histogram2d(sx, sy, bins=[x_bins, y_bins], density=True)

        # Suppress any 2D cell where 0 < raw_count < min_count per Stats NZ IDI rules
        suppressed_mask = (h_raw_cnt > 0) & (h_raw_cnt < min_count)
        h_raw_den = np.where(suppressed_mask, np.nan, h_raw_den)
        vmax = max(float(np.nanmax(h_raw_den)), float(np.nanmax(h_syn_den)), 1e-12)

        if include_scatter:
            fig, axes = plt.subplots(1, 3, figsize=(17, 5))
            ax1 = axes[0]
            ax1.scatter(rx, ry, alpha=0.35, s=18, color="#2b7bba", label="Raw")
            ax1.scatter(sx, sy, alpha=0.35, s=18, color="#e66101", label="Synthetic")
            ax1.set_xlabel(col1)
            ax1.set_ylabel(col2)
            ax1.set_title(f"Scatter: {col1} vs {col2}")
            ax1.legend()
            density_axes = axes[1:]
        else:
            fig, density_axes = plt.subplots(1, 2, figsize=(12, 5))

        for ax, den_mat, title in zip(
            density_axes,
            [h_raw_den, h_syn_den],
            [f"Raw 2D Density: {col1} vs {col2}", f"Synthetic 2D Density: {col1} vs {col2}"],
        ):
            mesh = ax.pcolormesh(x_bins, y_bins, den_mat.T, cmap="YlGnBu", vmin=0.0, vmax=vmax)
            fig.colorbar(mesh, ax=ax, label="Density")
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
    include_scatter: bool = False,
    min_count: int or None = None,
    config_or_path=None,
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
      - min_count: Minimum raw units per cell/category (default from `config_or_path`
        verification settings or 6).
      - config_or_path: Optional YAML path or config dict containing `verification` settings.

    Returns:
      - List of saved PNG file paths.
    """
    if min_count is None:
        if config_or_path is not None:
            from syspop.confidential.config import resolve_verification_config

            min_count = resolve_verification_config(config_or_path)["min_count"]
        else:
            min_count = 6

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if focus_column is not None:
        pairs = [(focus_column, c) for c in columns if c != focus_column]
    else:
        pairs = list(combinations(columns, 2))

    saved_paths = []
    for col1, col2 in pairs:
        path = _plot_single_pair_comparison(
            raw_df,
            syn_df,
            col1,
            col2,
            output_dir,
            include_scatter=include_scatter,
            min_count=min_count,
        )
        saved_paths.append(path)

    return saved_paths
