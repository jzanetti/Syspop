import numpy as np


def rr3_round(value: int or float, base: int = 3, rng: np.random.Generator or None = None) -> int:
    """
    Random Rounding to `base` (e.g. RR3 when `base=3`):
    - Values that are already multiples of `base` are unchanged.
    - Other values are rounded to the nearest multiple of `base` with probability
      inversely proportional to distance.
    """
    if rng is None:
        rng = np.random.default_rng()

    n = int(round(value))
    if n <= 0:
        return 0

    remainder = n % base
    if remainder == 0:
        return n

    lower = n - remainder
    upper = lower + base
    prob_upper = remainder / float(base)
    if rng.random() < prob_upper:
        return int(upper)
    return int(lower)


def apply_count_rounding(
    count: int,
    rules: dict,
    rng: np.random.Generator or None = None,
) -> int:
    """
    Apply random rounding using `rounding_base` (e.g. 3 for RR3).
    If `rounding_base` is None, False, 0, or 1, rounding is switched off and exact count is returned.
    """
    base = rules.get("rounding_base", 3)
    if base is None or base is False:
        return int(count)
    if isinstance(base, str):
        s = base.strip().lower()
        if s in ("none", "null", "false", "off", "0", "1", ""):
            return int(count)
        base = int(s)

    base_int = int(base)
    if base_int <= 1:
        return int(count)
    return rr3_round(count, base=base_int, rng=rng)


def check_subcell_violation(
    cluster_indices: np.ndarray,
    onehot_matrices: dict,
    min_cell_count: int,
    global_cat_counts: dict or None = None,
) -> bool:
    """
    Check if any categorical feature in the cluster has a non-zero contributing unit count
    strictly between 0 and `min_cell_count`.
    """
    if min_cell_count <= 1:
        return False

    for feat_name, mat in onehot_matrices.items():
        sub_counts = (mat[cluster_indices] > 0).sum(axis=0)
        for col_idx, c_val in enumerate(sub_counts):
            if 0 < c_val < min_cell_count:
                if global_cat_counts is not None:
                    if global_cat_counts[feat_name][col_idx] < min_cell_count:
                        continue
                return True
    return False


def confidentialise_category_proportions(
    cat_matrix: np.ndarray,
    rules: dict,
    rng: np.random.Generator or None = None,
) -> np.ndarray:
    """
    Compute confidentialised category proportions for a single categorical variable within a cluster.
    `cat_matrix` has shape `(n_group_records, n_categories)`.

    Supported `subcell_action` options:
      - 'reassign_or_merge' or 'merge' (Option 1 - Default):
        Clusters are formed/merged so non-zero categories meet `min_cell_count`.
      - 'suppress_cluster' (Option 2a):
        Returns raw proportions here; the caller suppresses the entire cluster if any sub-cell violates `min_cell_count`.
      - 'suppress_cell_nan' (Option 2b):
        Sets any violating sub-cell (0 < count < `min_cell_count`) to `np.nan`.
      - 'rr3_and_suppress' (Option 3):
        Masks any sub-cell with 0 < count < `min_cell_count` to 0.0, applies random rounding to remaining
        sub-cells >= `min_cell_count`, and re-normalizes proportions to sum to 1.0.
      - 'none':
        Raw proportions without modification.
    """
    mat = np.asarray(cat_matrix, dtype=float)
    if mat.ndim == 1:
        counts = mat.copy()
        contributing = counts.copy()
    else:
        counts = mat.sum(axis=0)
        contributing = (mat > 0).sum(axis=0)

    action = str(rules.get("subcell_action", "reassign_or_merge")).strip().lower()
    min_cell = int(rules.get("min_cell_count", 6))

    if action in ("none", "suppress_cluster"):
        total = counts.sum()
        return counts / total if total > 0 else np.ones_like(counts) / len(counts)

    if action == "suppress_cell_nan":
        total = counts.sum()
        props = counts / total if total > 0 else np.ones_like(counts) / len(counts)
        small_mask = (contributing > 0) & (contributing < min_cell)
        props[small_mask] = np.nan
        return props

    # For 'reassign_or_merge', 'merge', and 'rr3_and_suppress':
    small_mask = (contributing > 0) & (contributing < min_cell)
    if small_mask.any() and (~small_mask & (contributing >= min_cell)).any():
        counts[small_mask] = 0.0

    if action == "rr3_and_suppress":
        for i in range(len(counts)):
            if counts[i] > 0:
                if abs(counts[i] - round(counts[i])) > 1e-6 and contributing[i] > 0:
                    rounded_contrib = float(apply_count_rounding(int(contributing[i]), rules, rng=rng))
                    counts[i] = counts[i] * (rounded_contrib / float(contributing[i]))
                else:
                    counts[i] = float(apply_count_rounding(int(round(counts[i])), rules, rng=rng))

    total = counts.sum()
    if total <= 0:
        raw_sums = mat.sum(axis=0) if mat.ndim > 1 else np.asarray(cat_matrix, dtype=float)
        raw_total = float(np.sum(raw_sums))
        return raw_sums / raw_total if raw_total > 0 else np.ones_like(counts) / len(counts)

    return counts / total
