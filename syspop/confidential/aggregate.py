import numpy as np
import pandas as pd
from syspop.confidential.config import (
    load_config,
    resolve_data_structure,
    resolve_weight_dataframe,
)
from syspop.confidential.rules import (
    apply_count_rounding,
    check_subcell_violation,
    confidentialise_category_proportions,
)


def prepare_encoded_features(
    df: pd.DataFrame,
    features_meta: dict,
    pre_encoded_categories: dict or None = None,
) -> tuple:
    """
    Convert continuous and categorical features into:
      1. Normalized feature matrix (max-normalized inside Data Lab)
      2. Weighted feature matrix for squared Euclidean distance calculation:
         - Continuous feature f with weight w_f is scaled by sqrt(w_f)
         - Categorical feature c with weight w_c is one-hot encoded and scaled by sqrt(w_c / 2)
           so that mismatching categories contribute w_c * (1/2) * (1^2 + 1^2) = w_c * 1.0
      3. Metadata dictionary containing max values and category column mappings.
    """
    n_rows = len(df)
    weighted_cols = []
    norm_dict = {}
    max_values = {}
    cat_mappings = {}
    onehot_matrices = {}

    used_col_names = set(
        f for f, spec in features_meta.items() if spec["type"] == "continuous"
    )
    num_cat_features = sum(
        1 for spec in features_meta.values() if spec["type"] == "categorical"
    )

    for feat_name, spec in features_meta.items():
        f_type = spec["type"]
        weight = float(spec.get("weight", 1.0))

        if f_type == "continuous":
            raw_vals = pd.to_numeric(df[feat_name], errors="coerce").fillna(0.0).to_numpy(dtype=float)
            max_val = float(np.max(np.abs(raw_vals))) if n_rows > 0 else 1.0
            if max_val == 0.0:
                max_val = 1.0
            norm_vals = raw_vals / max_val
            max_values[feat_name] = max_val
            norm_dict[feat_name] = norm_vals
            weighted_cols.append(norm_vals[:, None] * np.sqrt(weight))

        elif f_type == "categorical":
            if pre_encoded_categories is not None and feat_name in pre_encoded_categories:
                cat_info = pre_encoded_categories[feat_name]
                col_names = cat_info["columns"]
                mat = df[col_names].fillna(0.0).to_numpy(dtype=float)
                cat_mappings[feat_name] = cat_info
            else:
                series_str = df[feat_name].fillna("Missing").astype(str)
                categories = list(dict.fromkeys(series_str.tolist()))
                col_names = []
                for cat_val in categories:
                    if num_cat_features == 1 and str(cat_val) not in used_col_names:
                        col_label = str(cat_val)
                    else:
                        col_label = f"{feat_name}_{cat_val}"
                    used_col_names.add(col_label)
                    col_names.append(col_label)

                mat = np.zeros((n_rows, len(categories)), dtype=float)
                for idx, cat_val in enumerate(categories):
                    mat[:, idx] = (series_str.to_numpy() == cat_val).astype(float)

                cat_mappings[feat_name] = {
                    "categories": categories,
                    "columns": col_names,
                }

            for idx, col_label in enumerate(cat_mappings[feat_name]["columns"]):
                norm_dict[col_label] = mat[:, idx]

            onehot_matrices[feat_name] = mat
            weighted_cols.append(mat * np.sqrt(weight * 0.5))

    if weighted_cols:
        X_weighted = np.hstack(weighted_cols)
    else:
        X_weighted = np.zeros((n_rows, 1), dtype=float)

    norm_df = pd.DataFrame(norm_dict, index=df.index)
    encoding_info = {
        "max_values": max_values,
        "cat_mappings": cat_mappings,
        "onehot_matrices": onehot_matrices,
    }
    return norm_df, X_weighted, encoding_info


def _extract_categorical_matrix(
    df: pd.DataFrame,
    feat_name: str,
    pre_encoded_categories: dict or None = None,
) -> tuple:
    """Return `(categories, onehot_or_prop_matrix)` for a categorical feature."""
    if pre_encoded_categories is not None and feat_name in pre_encoded_categories:
        cat_info = pre_encoded_categories[feat_name]
        col_names = cat_info["columns"]
        mat = df[col_names].fillna(0.0).to_numpy(dtype=float)
        return list(cat_info["categories"]), mat

    series_str = df[feat_name].fillna("Missing").astype(str)
    categories = list(dict.fromkeys(series_str.tolist()))
    mat = np.zeros((len(df), len(categories)), dtype=float)
    for idx, cat_val in enumerate(categories):
        mat[:, idx] = (series_str.to_numpy() == cat_val).astype(float)
    return categories, mat


def add_correlation_features(
    df: pd.DataFrame,
    features_meta: dict,
    correlations: list or None = None,
    pre_encoded_categories: dict or None = None,
) -> tuple:
    """
    Add correlation/interaction columns to `df` for configured feature pairs prior to
    nearest-neighbour clustering:
      - Continuous x Continuous : standardized z-score product (z_f1 * z_f2)
      - Continuous x Categorical: scaled non-negative continuous value per category
      - Categorical x Categorical: joint category indicator / cross-product columns

    Returns:
        tuple: `(df_with_corr, corr_features_meta)` where `corr_features_meta` describes
        the added correlation columns for distance encoding.
    """
    if not correlations:
        return df, {}

    out_df = df.copy()
    corr_features_meta = {}

    for corr_spec in correlations:
        pair = corr_spec.get("features", [])
        if len(pair) != 2:
            continue
        f1, f2 = pair[0], pair[1]
        if f1 not in features_meta or f2 not in features_meta:
            continue

        t1 = features_meta[f1]["type"]
        t2 = features_meta[f2]["type"]
        weight = float(corr_spec.get("weight", 1.0))
        base_name = corr_spec.get("name", f"corr_{f1}_{f2}")

        if t1 == "continuous" and t2 == "continuous":
            x1 = pd.to_numeric(out_df[f1], errors="coerce").fillna(0.0).to_numpy(dtype=float)
            x2 = pd.to_numeric(out_df[f2], errors="coerce").fillna(0.0).to_numpy(dtype=float)
            s1, s2 = float(np.std(x1)), float(np.std(x2))
            z1 = (x1 - float(np.mean(x1))) / s1 if s1 > 0 else np.zeros_like(x1)
            z2 = (x2 - float(np.mean(x2))) / s2 if s2 > 0 else np.zeros_like(x2)
            out_df[base_name] = z1 * z2
            corr_features_meta[base_name] = {
                "type": "continuous",
                "weight": weight,
                "non_negative": False,
                "is_correlation": True,
            }
        elif t1 == "continuous" or t2 == "continuous":
            cont_f, cat_f = (f1, f2) if t1 == "continuous" else (f2, f1)
            x_c = pd.to_numeric(out_df[cont_f], errors="coerce").fillna(0.0).to_numpy(dtype=float)
            x_min, x_max = float(np.min(x_c)), float(np.max(x_c))
            u_cont = (
                0.1 + 0.9 * (x_c - x_min) / (x_max - x_min)
                if x_max > x_min
                else np.ones_like(x_c)
            )
            cats, mat = _extract_categorical_matrix(out_df, cat_f, pre_encoded_categories)
            for idx, cat_val in enumerate(cats):
                col_name = f"{base_name}_{cat_val}"
                out_df[col_name] = u_cont * mat[:, idx]
                corr_features_meta[col_name] = {
                    "type": "continuous",
                    "weight": weight * 0.5,
                    "non_negative": True,
                    "is_correlation": True,
                }
        else:
            if pre_encoded_categories is None or (
                f1 not in pre_encoded_categories and f2 not in pre_encoded_categories
            ):
                s1 = out_df[f1].fillna("Missing").astype(str)
                s2 = out_df[f2].fillna("Missing").astype(str)
                out_df[base_name] = s1 + "__" + s2
                corr_features_meta[base_name] = {
                    "type": "categorical",
                    "weight": weight,
                    "is_correlation": True,
                }
            else:
                cats1, mat1 = _extract_categorical_matrix(out_df, f1, pre_encoded_categories)
                cats2, mat2 = _extract_categorical_matrix(out_df, f2, pre_encoded_categories)
                for i, c1 in enumerate(cats1):
                    for j, c2 in enumerate(cats2):
                        col_name = f"{base_name}_{c1}_{c2}"
                        out_df[col_name] = mat1[:, i] * mat2[:, j]
                        corr_features_meta[col_name] = {
                            "type": "continuous",
                            "weight": weight * 0.5,
                            "non_negative": True,
                            "is_correlation": True,
                        }

    return out_df, corr_features_meta


def compute_pairwise_distances(df: pd.DataFrame, config_or_path=None) -> np.ndarray:
    """
    Compute the pairwise weighted squared distance matrix between all unit records in `df`
    following Section 1.1 of the Synthetic Data Production Algorithm.
    """
    cfg = load_config(config_or_path)
    data_struct = resolve_data_structure(df, cfg)
    df_with_corr, corr_features_meta = add_correlation_features(
        df,
        data_struct["features"],
        correlations=data_struct.get("correlations", []),
    )
    dist_features_meta = {**data_struct["features"], **corr_features_meta}
    _, X_weighted, _ = prepare_encoded_features(df_with_corr, dist_features_meta)
    diff = X_weighted[:, None, :] - X_weighted[None, :, :]
    return np.sum(diff ** 2, axis=-1)


def _count_record_subcell_violations(
    cluster_indices: np.ndarray,
    onehot_matrices: dict,
    min_cell_count: int,
    global_cat_counts: dict or None = None,
) -> int:
    """Count how many categorical columns in `cluster_indices` have 0 < count < min_cell_count."""
    violations = 0
    for feat_name, mat in onehot_matrices.items():
        counts = (mat[cluster_indices] > 0).sum(axis=0)
        for col_idx, c_val in enumerate(counts):
            if 0 < c_val < min_cell_count:
                if global_cat_counts is not None and global_cat_counts[feat_name][col_idx] < min_cell_count:
                    continue
                violations += 1
    return violations


def _compute_subcell_deficit(
    cluster_indices: np.ndarray,
    onehot_matrices: dict,
    min_cell_count: int,
    global_cat_counts: dict or None = None,
) -> int:
    """
    Compute total sub-cell deficit across all categorical columns in `cluster_indices`.
    For each category with 0 < count < min_cell_count, deficit is min(count, min_cell_count - count),
    measuring the minimum number of records needed to reach either 0 or min_cell_count.
    """
    deficit = 0
    for feat_name, mat in onehot_matrices.items():
        counts = (mat[cluster_indices] > 0).sum(axis=0)
        for col_idx, c_val in enumerate(counts):
            if 0 < c_val < min_cell_count:
                if global_cat_counts is not None and global_cat_counts[feat_name][col_idx] < min_cell_count:
                    continue
                deficit += int(min(c_val, min_cell_count - c_val))
    return deficit


def _record_fits_cluster_categories(
    rec_idx: int,
    cluster_indices: np.ndarray,
    onehot_matrices: dict,
    min_cell_count: int,
) -> bool:
    """
    Return True if adding `rec_idx` to `cluster_indices` does NOT introduce any new
    small sub-cell (i.e. for every category that `rec_idx` has > 0, the cluster already
    has >= `min_cell_count` contributing records, or adding `rec_idx` brings it to >= `min_cell_count`).
    """
    for mat in onehot_matrices.values():
        rec_cats = np.flatnonzero(mat[rec_idx] > 0)
        if len(rec_cats) == 0:
            continue
        cluster_counts = (mat[cluster_indices] > 0).sum(axis=0)
        for col_idx in rec_cats:
            if cluster_counts[col_idx] + 1 < min_cell_count:
                return False
    return True


def _resolve_small_subcells_by_reassignment(
    clusters: list,
    X_weighted: np.ndarray,
    onehot_matrices: dict,
    min_count: int,
    min_cell_count: int,
    global_cat_counts: dict,
    max_passes: int = 5,
) -> list:
    """
    Option 1 ('reassign_or_merge') helper:
    Eliminate small categorical sub-cells (0 < count < min_cell_count) across clusters by:
      1. Moving records that belong to a small sub-cell out of clusters with > min_count
         into the nearest cluster where those categories are already >= min_cell_count.
      2. Swapping minority-category records between clusters when doing so strictly reduces
         the number of small sub-cell violations.
      3. Merging any remaining clusters that share the same small sub-cell category so their
         combined sub-cell count reaches >= min_cell_count.
    """
    if not onehot_matrices or min_cell_count <= 1 or len(clusters) < 2:
        return clusters

    for _ in range(max_passes):
        changed = False
        centroids = np.array([X_weighted[c].mean(axis=0) for c in clusters])

        for c_idx in range(len(clusters)):
            c_arr = clusters[c_idx]
            cur_v_a = _count_record_subcell_violations(
                c_arr, onehot_matrices, min_cell_count, global_cat_counts
            )
            if cur_v_a == 0:
                continue

            # Find records in cluster c_idx that contribute to any small sub-cell (0 < count < min_cell_count)
            small_positions = set()
            for feat_name, mat in onehot_matrices.items():
                sub_mat = mat[c_arr] > 0
                counts = sub_mat.sum(axis=0)
                for col_k in np.flatnonzero((counts > 0) & (counts < min_cell_count)):
                    if global_cat_counts[feat_name][col_k] < min_cell_count:
                        continue
                    for pos in np.flatnonzero(sub_mat[:, col_k]):
                        small_positions.add(int(pos))

            d_other = np.sum((centroids - centroids[c_idx]) ** 2, axis=1)
            d_other[c_idx] = np.inf
            other_order = [int(i) for i in np.argsort(d_other) if int(i) != c_idx]

            small_records = [int(c_arr[p]) for p in sorted(small_positions)]
            for rec_a in small_records:
                cur_a = clusters[c_idx]
                pos_matches = np.flatnonzero(cur_a == rec_a)
                if len(pos_matches) == 0:
                    continue
                pos_a = int(pos_matches[0])
                cur_d_a = _compute_subcell_deficit(
                    cur_a, onehot_matrices, min_cell_count, global_cat_counts
                )
                if cur_d_a == 0:
                    break

                without_a = np.delete(cur_a, pos_a)
                d_without_a = _compute_subcell_deficit(
                    without_a, onehot_matrices, min_cell_count, global_cat_counts
                )

                # Step A: If cluster A has > min_count records, move rec_a to a nearby compatible cluster B
                if len(cur_a) > min_count:
                    moved = False
                    for b_idx in other_order:
                        cur_b = clusters[b_idx]
                        d_b_before = _compute_subcell_deficit(
                            cur_b, onehot_matrices, min_cell_count, global_cat_counts
                        )
                        with_a_in_b = np.append(cur_b, rec_a)
                        d_b_after = _compute_subcell_deficit(
                            with_a_in_b, onehot_matrices, min_cell_count, global_cat_counts
                        )
                        if d_without_a + d_b_after < cur_d_a + d_b_before:
                            clusters[c_idx] = without_a
                            clusters[b_idx] = with_a_in_b
                            centroids[c_idx] = X_weighted[clusters[c_idx]].mean(axis=0)
                            centroids[b_idx] = X_weighted[clusters[b_idx]].mean(axis=0)
                            changed = True
                            moved = True
                            break
                    if moved:
                        continue

                # Step B: Swap rec_a with a record rec_b from another cluster B
                swapped = False
                for b_idx in other_order:
                    cur_b = clusters[b_idx]
                    d_b_before = _compute_subcell_deficit(
                        cur_b, onehot_matrices, min_cell_count, global_cat_counts
                    )
                    for pos_b in range(len(cur_b)):
                        rec_b = cur_b[pos_b]
                        new_a = np.append(without_a, rec_b)
                        new_b = np.append(np.delete(cur_b, pos_b), rec_a)

                        d_a_after = _compute_subcell_deficit(
                            new_a, onehot_matrices, min_cell_count, global_cat_counts
                        )
                        d_b_after = _compute_subcell_deficit(
                            new_b, onehot_matrices, min_cell_count, global_cat_counts
                        )
                        if d_a_after + d_b_after < cur_d_a + d_b_before:
                            clusters[c_idx] = new_a
                            clusters[b_idx] = new_b
                            centroids[c_idx] = X_weighted[clusters[c_idx]].mean(axis=0)
                            centroids[b_idx] = X_weighted[clusters[b_idx]].mean(axis=0)
                            changed = True
                            swapped = True
                            break
                    if swapped:
                        break

        if not changed:
            break

    # Final pass: if any cluster still has a small sub-cell, merge it with the best partner cluster
    # that reduces the total violation count (or nearest neighbour if needed)
    merged_any = True
    while merged_any and len(clusters) > 1:
        merged_any = False
        for idx, c_indices in enumerate(clusters):
            v_cur = _count_record_subcell_violations(
                c_indices, onehot_matrices, min_cell_count, global_cat_counts
            )
            if v_cur > 0:
                centroids = np.array([X_weighted[c].mean(axis=0) for c in clusters])
                d_to_others = np.sum((centroids - centroids[idx]) ** 2, axis=1)
                d_to_others[idx] = np.inf

                # Prefer merging with a cluster where combined violations decrease
                best_other = None
                best_score = (999999, np.inf)
                for other_idx in np.argsort(d_to_others):
                    if other_idx == idx:
                        continue
                    v_other = _count_record_subcell_violations(
                        clusters[other_idx], onehot_matrices, min_cell_count, global_cat_counts
                    )
                    combined = np.concatenate([c_indices, clusters[other_idx]])
                    v_comb = _count_record_subcell_violations(
                        combined, onehot_matrices, min_cell_count, global_cat_counts
                    )
                    # Score by (resulting violations, distance)
                    cand_score = (v_comb, float(d_to_others[other_idx]))
                    if v_comb < v_cur + v_other and cand_score < best_score:
                        best_score = cand_score
                        best_other = int(other_idx)

                if best_other is None:
                    best_other = int(np.argmin(d_to_others))

                target_idx = min(idx, best_other)
                remove_idx = max(idx, best_other)
                clusters[target_idx] = np.concatenate([clusters[target_idx], clusters[remove_idx]])
                clusters.pop(remove_idx)
                merged_any = True
                break

    return clusters


def _repair_cluster_from_pool(
    c_arr: np.ndarray,
    pool_arr: np.ndarray,
    X_weighted: np.ndarray,
    onehot_matrices: dict,
    min_cell_count: int,
    global_cat_counts: dict,
    max_iters: int = 20,
) -> tuple:
    """
    Repair any small categorical sub-cells (0 < count < min_cell_count) inside `c_arr`
    while keeping `len(c_arr)` unchanged by swapping records with `pool_arr`:
      - Option A (Top-up): Pull `need = min_cell_count - cnt` nearest records of that
        category from `pool_arr` and push `need` farthest non-category records out to `pool_arr`.
      - Option B (Purge): Push the `cnt` minority records out to `pool_arr` and pull
        `cnt` nearest compatible records from `pool_arr`.
    Picks whichever option reduces `_compute_subcell_deficit` with the tightest cluster SSE.
    """
    if not onehot_matrices or min_cell_count <= 1 or len(pool_arr) == 0:
        return c_arr, pool_arr

    for _ in range(max_iters):
        cur_deficit = _compute_subcell_deficit(
            c_arr, onehot_matrices, min_cell_count, global_cat_counts
        )
        if cur_deficit == 0:
            break

        # Find the violating cell with the smallest deficit
        best_cell = None
        best_cell_def = 10**9
        for feat_name, mat in onehot_matrices.items():
            counts = (mat[c_arr] > 0).sum(axis=0)
            for col_k in np.flatnonzero((counts > 0) & (counts < min_cell_count)):
                if global_cat_counts[feat_name][col_k] < min_cell_count:
                    continue
                cnt = int(counts[col_k])
                d_val = min(cnt, min_cell_count - cnt)
                if d_val < best_cell_def:
                    best_cell_def = d_val
                    best_cell = (feat_name, int(col_k), cnt)

        if best_cell is None:
            break

        feat_name, col_k, cnt = best_cell
        mat = onehot_matrices[feat_name] > 0
        c_has_mask = mat[c_arr, col_k]
        p_has_mask = mat[pool_arr, col_k]

        in_c_has = c_arr[c_has_mask]
        in_c_not = c_arr[~c_has_mask]
        pool_has = pool_arr[p_has_mask]
        pool_not = pool_arr[~p_has_mask]

        centroid = X_weighted[c_arr].mean(axis=0)
        candidates = []

        # Option A: Top-up category col_k to min_cell_count
        need = min_cell_count - cnt
        if len(pool_has) >= need and len(in_c_not) >= need:
            d_pool = np.sum((X_weighted[pool_has] - centroid) ** 2, axis=1)
            add_recs = pool_has[np.argsort(d_pool)[:need]]

            d_out = np.sum((X_weighted[in_c_not] - centroid) ** 2, axis=1)
            drop_recs = in_c_not[np.argsort(-d_out)[:need]]
            keep_not = in_c_not[np.argsort(-d_out)[need:]]

            cand_a = np.concatenate([in_c_has, keep_not, add_recs])
            def_a = _compute_subcell_deficit(
                cand_a, onehot_matrices, min_cell_count, global_cat_counts
            )
            if def_a < cur_deficit:
                sse_a = float(np.sum((X_weighted[cand_a] - X_weighted[cand_a].mean(axis=0)) ** 2))
                candidates.append((def_a, sse_a, cand_a, add_recs, drop_recs))

        # Option B: Purge category col_k down to 0
        if len(pool_not) >= cnt:
            d_pool_not = np.sum((X_weighted[pool_not] - centroid) ** 2, axis=1)
            # Prefer pool_not records that fit existing categories in in_c_not
            fits = np.array(
                [
                    _record_fits_cluster_categories(int(r), in_c_not, onehot_matrices, min_cell_count)
                    for r in pool_not
                ],
                dtype=bool,
            )
            order = np.argsort(d_pool_not + np.where(fits, 0.0, 1e6))
            add_recs_b = pool_not[order[:cnt]]
            drop_recs_b = in_c_has

            cand_b = np.concatenate([in_c_not, add_recs_b])
            def_b = _compute_subcell_deficit(
                cand_b, onehot_matrices, min_cell_count, global_cat_counts
            )
            if def_b < cur_deficit:
                sse_b = float(np.sum((X_weighted[cand_b] - X_weighted[cand_b].mean(axis=0)) ** 2))
                candidates.append((def_b, sse_b, cand_b, add_recs_b, drop_recs_b))

        if not candidates:
            break

        _, _, best_cand, added, dropped = min(candidates, key=lambda x: (x[0], x[1]))
        c_arr = best_cand
        added_set = set(int(x) for x in added)
        pool_keep = np.array([r for r in pool_arr if int(r) not in added_set], dtype=int)
        pool_arr = np.concatenate([pool_keep, dropped.astype(int)])

    return c_arr, pool_arr


def _form_subcell_compliant_clusters(
    X_weighted: np.ndarray,
    min_count: int,
    leftover_action: str,
    onehot_matrices: dict,
    min_cell_count: int,
    candidate_pool_size: int = 500,
) -> list:
    """
    Form clusters of size >= `min_count` where every categorical sub-cell has either 0
    or >= `min_cell_count` records:
      1. Build each nearest-neighbour cluster of size `min_count` and directly repair any
         small sub-cell (e.g. turning 17M+3F into 14M+6F or 20M+0F) using nearest records
         from the unassigned pool.
      2. Assign any leftover records (< `min_count`) according to `leftover_action`, preferring
         clusters that remain sub-cell compliant.
      3. Run cross-cluster reassignment/swap repair so both `min_count` and `min_cell_count`
         are satisfied without unnecessary cluster merging.
    """
    n_rows = X_weighted.shape[0]
    global_cat_counts = {
        f: (mat > 0).sum(axis=0) for f, mat in onehot_matrices.items()
    }

    # Check if categorical matrices are binary (individual level) vs fractional (family/household level)
    all_binary = all(
        np.all((mat == 0.0) | (mat == 1.0)) for mat in onehot_matrices.values()
    )

    if not all_binary:
        base_clusters = _standard_nn_clusters(
            X_weighted, min_count, leftover_action, candidate_pool_size
        )
        return _resolve_small_subcells_by_reassignment(
            base_clusters, X_weighted, onehot_matrices, min_count, min_cell_count, global_cat_counts
        )

    unassigned = np.arange(n_rows, dtype=int)
    clusters = []

    while len(unassigned) >= min_count:
        u_len = len(unassigned)
        X_u = X_weighted[unassigned]

        if u_len == min_count:
            clusters.append(unassigned.copy())
            unassigned = np.array([], dtype=int)
            break

        if u_len <= candidate_pool_size:
            cand_positions = np.arange(u_len)
        else:
            stride = max(1, u_len // candidate_pool_size)
            cand_positions = np.arange(0, u_len, stride)[:candidate_pool_size]

        X_cand = X_u[cand_positions]
        dists = np.sum((X_cand[:, None, :] - X_u[None, :, :]) ** 2, axis=-1)

        nearest_part = np.argpartition(dists, kth=min_count - 1, axis=1)[:, :min_count]
        row_idx = np.arange(len(cand_positions))[:, None]
        group_scores = np.sum(dists[row_idx, nearest_part], axis=1)

        best_cand_idx = int(np.argmin(group_scores))
        chosen_local = nearest_part[best_cand_idx]
        chosen_local = chosen_local[np.argsort(dists[best_cand_idx, chosen_local])]

        c_arr = unassigned[chosen_local]
        mask = np.ones(u_len, dtype=bool)
        mask[chosen_local] = False
        pool_arr = unassigned[mask]

        c_arr, pool_arr = _repair_cluster_from_pool(
            c_arr,
            pool_arr,
            X_weighted,
            onehot_matrices,
            min_cell_count,
            global_cat_counts,
        )
        clusters.append(c_arr)
        unassigned = pool_arr

    # Handle leftover records (< min_count)
    if len(unassigned) > 0 and len(clusters) > 0:
        action = str(leftover_action).strip().lower()
        if action == "assign_nearest":
            centroids = np.array([X_weighted[c].mean(axis=0) for c in clusters])
            still_unassigned = []
            for rec_idx in unassigned:
                compatible_clusters = [
                    c_i
                    for c_i, c_indices in enumerate(clusters)
                    if _record_fits_cluster_categories(
                        int(rec_idx), c_indices, onehot_matrices, min_cell_count
                    )
                ]
                if compatible_clusters:
                    comp_centroids = centroids[compatible_clusters]
                    d = np.sum((comp_centroids - X_weighted[rec_idx]) ** 2, axis=1)
                    chosen_c = compatible_clusters[int(np.argmin(d))]
                    clusters[chosen_c] = np.append(clusters[chosen_c], rec_idx)
                    centroids[chosen_c] = X_weighted[clusters[chosen_c]].mean(axis=0)
                else:
                    still_unassigned.append(int(rec_idx))

            for rec_idx in still_unassigned:
                d = np.sum((centroids - X_weighted[rec_idx]) ** 2, axis=1)
                chosen_c = int(np.argmin(d))
                clusters[chosen_c] = np.append(clusters[chosen_c], rec_idx)
                centroids[chosen_c] = X_weighted[clusters[chosen_c]].mean(axis=0)
        elif action == "suppress":
            pass
        else:
            raise ValueError(
                f"Unsupported leftover_action '{leftover_action}'. Use 'assign_nearest' or 'suppress'."
            )

    return _resolve_small_subcells_by_reassignment(
        clusters, X_weighted, onehot_matrices, min_count, min_cell_count, global_cat_counts
    )


def _standard_nn_clusters(
    X_weighted: np.ndarray,
    min_count: int,
    leftover_action: str,
    candidate_pool_size: int = 500,
) -> list:
    """Standard greedy nearest-neighbour clustering into groups of >= `min_count`."""
    n_rows = X_weighted.shape[0]
    unassigned = np.arange(n_rows, dtype=int)
    clusters = []

    while len(unassigned) >= min_count:
        u_len = len(unassigned)
        X_u = X_weighted[unassigned]

        if u_len == min_count:
            clusters.append(unassigned.copy())
            unassigned = np.array([], dtype=int)
            break

        if u_len <= candidate_pool_size:
            cand_positions = np.arange(u_len)
        else:
            stride = max(1, u_len // candidate_pool_size)
            cand_positions = np.arange(0, u_len, stride)[:candidate_pool_size]

        X_cand = X_u[cand_positions]
        dists = np.sum((X_cand[:, None, :] - X_u[None, :, :]) ** 2, axis=-1)

        nearest_part = np.argpartition(dists, kth=min_count - 1, axis=1)[:, :min_count]
        row_idx = np.arange(len(cand_positions))[:, None]
        group_scores = np.sum(dists[row_idx, nearest_part], axis=1)

        best_cand_idx = int(np.argmin(group_scores))
        chosen_local = nearest_part[best_cand_idx]
        chosen_local = chosen_local[np.argsort(dists[best_cand_idx, chosen_local])]

        clusters.append(unassigned[chosen_local])

        mask = np.ones(u_len, dtype=bool)
        mask[chosen_local] = False
        unassigned = unassigned[mask]

    if len(unassigned) > 0 and len(clusters) > 0:
        action = str(leftover_action).strip().lower()
        if action == "assign_nearest":
            centroids = np.array([X_weighted[c].mean(axis=0) for c in clusters])
            for rec_idx in unassigned:
                d_to_clusters = np.sum((centroids - X_weighted[rec_idx]) ** 2, axis=1)
                nearest_c = int(np.argmin(d_to_clusters))
                clusters[nearest_c] = np.append(clusters[nearest_c], rec_idx)
                centroids[nearest_c] = X_weighted[clusters[nearest_c]].mean(axis=0)
        elif action == "suppress":
            pass
        else:
            raise ValueError(f"Unsupported leftover_action '{leftover_action}'. Use 'assign_nearest' or 'suppress'.")

    return clusters


def nearest_neighbour_grouping(
    X_weighted: np.ndarray,
    min_count: int,
    leftover_action: str = "assign_nearest",
    onehot_matrices: dict or None = None,
    subcell_action: str = "reassign_or_merge",
    min_cell_count: int = 6,
    candidate_pool_size: int = 500,
) -> list:
    """
    Group unit records into clusters of size >= `min_count` using Nearest Neighbour distances.
    Supports configurable `subcell_action` for small categorical cells (< `min_cell_count`):
      - 'reassign_or_merge' (Option 1 - Default): Form compliant clusters and reassign/merge
        so no released cluster has 0 < category_count < min_cell_count.
      - 'merge': Standard NN clustering followed by merging clusters with small categorical cells.
      - 'suppress_cluster' (Option 2a): Suppress entire cluster if any category < min_cell_count.
      - 'suppress_cell_nan' (Option 2b): Suppress only the small category proportion to NaN.
      - 'rr3_and_suppress' (Option 3): Mask small categories (< min_cell_count) to 0.0 and apply random rounding.
      - 'none': No adjustment.
    """
    n_rows = X_weighted.shape[0]
    min_count = max(1, int(min_count))

    if n_rows < min_count:
        return []

    action = str(subcell_action).strip().lower()

    if action == "reassign_or_merge" and onehot_matrices and min_cell_count > 1:
        return _form_subcell_compliant_clusters(
            X_weighted=X_weighted,
            min_count=min_count,
            leftover_action=leftover_action,
            onehot_matrices=onehot_matrices,
            min_cell_count=min_cell_count,
            candidate_pool_size=candidate_pool_size,
        )

    clusters = _standard_nn_clusters(
        X_weighted=X_weighted,
        min_count=min_count,
        leftover_action=leftover_action,
        candidate_pool_size=candidate_pool_size,
    )

    if action == "merge" and onehot_matrices and min_cell_count > 1 and len(clusters) > 1:
        global_cat_counts = {
            f: (mat > 0).sum(axis=0) for f, mat in onehot_matrices.items()
        }
        return _resolve_small_subcells_by_reassignment(
            clusters, X_weighted, onehot_matrices, min_count, min_cell_count, global_cat_counts
        )

    return clusters


def _compute_weighted_mean_std(vals: np.ndarray, weights: np.ndarray or None = None) -> tuple:
    """Compute (mean, std) with optional survey weights."""
    if len(vals) == 0:
        return np.nan, np.nan
    if weights is None:
        mean_val = float(np.mean(vals))
        std_val = float(np.std(vals, ddof=1)) if len(vals) > 1 else 0.0
        return mean_val, std_val

    w = np.maximum(0.0, np.asarray(weights, dtype=float))
    w_sum = float(np.sum(w))
    if w_sum <= 0:
        mean_val = float(np.mean(vals))
        std_val = float(np.std(vals, ddof=1)) if len(vals) > 1 else 0.0
        return mean_val, std_val

    mean_val = float(np.sum(w * vals) / w_sum)
    if len(vals) > 1:
        var_val = float(np.sum(w * (vals - mean_val) ** 2) / w_sum) * (len(vals) / (len(vals) - 1.0))
        std_val = float(np.sqrt(max(0.0, var_val)))
    else:
        std_val = 0.0
    return mean_val, std_val


def aggregate_unit_records(
    df: pd.DataFrame,
    config_or_path=None,
    features_meta: dict or None = None,
    pre_encoded_categories: dict or None = None,
    weight_col: str or None = None,
    return_details: bool = False,
    correlations: list or None = None,
) -> pd.DataFrame or tuple:
    """
    Step 1 of the Synthetic Data Production Algorithm:
    Aggregate confidential unit-record microdata into clusters using the weighted
    nearest-neighbour approach while enforcing configured confidentiality rules.
    Supports both regular unweighted unit records and survey records with survey weights.
    """
    cfg = load_config(config_or_path)
    rules = cfg["output_rules"]
    seed = cfg.get("synthesis", {}).get("random_seed", rules.get("random_seed", None))
    rng = np.random.default_rng(seed)

    data_struct = resolve_data_structure(df, cfg)
    if features_meta is None:
        features_meta = {
            k: v for k, v in data_struct["features"].items() if k != weight_col
        }

    has_weights = bool(weight_col is not None and weight_col in df.columns)
    if has_weights:
        all_unit_weights = (
            pd.to_numeric(df[weight_col], errors="coerce")
            .fillna(1.0)
            .clip(lower=0.0)
            .to_numpy(dtype=float)
        )
    else:
        all_unit_weights = None

    active_correlations = (
        data_struct.get("correlations", []) if correlations is None else correlations
    )
    df_for_dist, corr_features_meta = add_correlation_features(
        df,
        features_meta,
        correlations=active_correlations,
        pre_encoded_categories=pre_encoded_categories,
    )
    dist_features_meta = {**features_meta, **corr_features_meta}

    norm_df, X_weighted, encoding_info = prepare_encoded_features(
        df_for_dist, dist_features_meta, pre_encoded_categories=pre_encoded_categories
    )
    real_onehot_matrices = {
        k: v for k, v in encoding_info["onehot_matrices"].items() if k in features_meta
    }

    min_group_count = int(rules["min_group_count"])
    min_cell_count = int(rules.get("min_cell_count", 6))
    subcell_action = str(rules.get("subcell_action", "reassign_or_merge")).strip().lower()

    clusters = nearest_neighbour_grouping(
        X_weighted=X_weighted,
        min_count=min_group_count,
        leftover_action=rules.get("leftover_action", "assign_nearest"),
        onehot_matrices=real_onehot_matrices,
        subcell_action=subcell_action,
        min_cell_count=min_cell_count,
    )

    rounding_base = rules.get("rounding_base", 3)
    rounding_enabled = rounding_base is not None and int(rounding_base) > 1

    rows = []
    kept_clusters = []
    cluster_audit_records = []

    assigned_set = set()
    cluster_seq = 1

    for c_indices in clusters:
        assigned_set.update(int(i) for i in c_indices)
        unrounded_n = len(c_indices)
        c_weights = all_unit_weights[c_indices] if all_unit_weights is not None else None
        rounded_n = apply_count_rounding(unrounded_n, rules, rng=rng)

        suppressed = False
        suppression_reason = ""
        # Confidentiality checks always evaluate UNWEIGHTED sample counts
        if unrounded_n < min_group_count:
            suppressed = True
            suppression_reason = f"Unweighted count ({unrounded_n}) < min_group_count ({min_group_count})"
        elif rounding_enabled and rounded_n < min_cell_count:
            suppressed = True
            suppression_reason = f"Rounded count ({rounded_n}) < min_cell_count ({min_cell_count})"
        elif rounded_n <= 0:
            suppressed = True
            suppression_reason = f"Rounded count ({rounded_n}) <= 0"
        elif subcell_action == "suppress_cluster" and check_subcell_violation(
            c_indices, real_onehot_matrices, min_cell_count
        ):
            suppressed = True
            suppression_reason = (
                f"Cluster contains categorical cell with 0 < unweighted count < {min_cell_count} "
                f"(subcell_action='suppress_cluster')"
            )

        sub_df = df.iloc[c_indices]
        row_data = {}
        cont_audit = {}
        cat_audit = {}

        # 1. Continuous features: resume un-normalized scale inside Data Lab (mean & std only; min/max are never exported)
        for feat_name, spec in features_meta.items():
            if spec["type"] != "continuous":
                continue
            s_num = pd.to_numeric(sub_df[feat_name], errors="coerce")
            valid_mask = s_num.notna().to_numpy()
            vals = s_num[valid_mask].to_numpy(dtype=float)
            w_valid = c_weights[valid_mask] if c_weights is not None else None
            raw_mean, raw_std = _compute_weighted_mean_std(vals, w_valid)

            row_data[feat_name] = raw_mean
            row_data[f"{feat_name}_std"] = raw_std

            cont_audit[feat_name] = {
                "raw_count": int(len(vals)),
                "min_group_count": min_group_count,
                "raw_mean": raw_mean,
                "exported_mean": raw_mean if not suppressed else np.nan,
                "raw_std": raw_std,
                "exported_std": raw_std if not suppressed else np.nan,
                "status": "PASS" if not suppressed else f"SUPPRESSED ({suppression_reason})",
            }

        # Export survey weight mean & std so Step 3 synthesizes the weight column on the synthetic sample
        if has_weights and weight_col not in row_data:
            w_mean, w_std = _compute_weighted_mean_std(c_weights, None)
            row_data[weight_col] = w_mean
            row_data[f"{weight_col}_std"] = w_std
            cont_audit[weight_col] = {
                "raw_count": int(len(c_weights)),
                "min_group_count": min_group_count,
                "raw_mean": w_mean,
                "exported_mean": w_mean if not suppressed else np.nan,
                "raw_std": w_std,
                "exported_std": w_std if not suppressed else np.nan,
                "status": "PASS" if not suppressed else f"SUPPRESSED ({suppression_reason})",
            }

        # 2. Categorical features: confidentialised proportions (unweighted threshold check + weighted proportions)
        for feat_name, spec in features_meta.items():
            if spec["type"] != "categorical":
                continue
            cat_info = encoding_info["cat_mappings"][feat_name]
            mat = encoding_info["onehot_matrices"][feat_name][c_indices]
            if c_weights is not None:
                raw_sums = (mat * c_weights[:, None]).sum(axis=0)
            else:
                raw_sums = mat.sum(axis=0)
            raw_total = float(raw_sums.sum())
            raw_props = raw_sums / raw_total if raw_total > 0 else np.zeros_like(raw_sums)
            contributing_counts = (mat > 0).sum(axis=0)

            props = confidentialise_category_proportions(
                mat, rules, rng=rng, unit_weights=c_weights
            )
            for col_idx, col_label in enumerate(cat_info["columns"]):
                cat_name = cat_info["categories"][col_idx]
                exp_prop = float(props[col_idx]) if pd.notna(props[col_idx]) else np.nan
                row_data[col_label] = exp_prop
                c_cnt = int(contributing_counts[col_idx])

                if suppressed:
                    sub_status = "SUPPRESSED"
                elif c_cnt == 0 or c_cnt >= min_cell_count or subcell_action == "none":
                    sub_status = "PASS"
                elif subcell_action == "suppress_cell_nan" and pd.isna(exp_prop):
                    sub_status = "SUPPRESSED (NaN)"
                elif exp_prop == 0.0:
                    sub_status = "SUPPRESSED (0.0)"
                else:
                    sub_status = "PASS"

                cat_audit[col_label] = {
                    "feature": feat_name,
                    "category": str(cat_name),
                    "unrounded_contributing_count": c_cnt,
                    "raw_sum": float(raw_sums[col_idx]),
                    "raw_proportion": float(raw_props[col_idx]),
                    "exported_proportion": exp_prop if not suppressed else np.nan,
                    "min_cell_count": min_cell_count,
                    "status": sub_status,
                }

        if not suppressed:
            cid = cluster_seq
            cluster_seq += 1
            row_with_id = {"cluster_id": int(cid)}
            row_with_id.update(row_data)
            row_with_id["count"] = int(rounded_n)
            rows.append(row_with_id)
            kept_clusters.append(c_indices)
            status_label = "RELEASED"
        else:
            cid = f"SUPPRESSED_{len(cluster_audit_records) + 1}"
            status_label = "SUPPRESSED"

        cluster_audit_records.append(
            {
                "cluster_id": cid,
                "status": status_label,
                "suppression_reason": suppression_reason,
                "indices": c_indices,
                "unrounded_count": int(unrounded_n),
                "min_group_count": int(min_group_count),
                "min_cell_count": int(min_cell_count),
                "rounded_count": int(rounded_n) if not suppressed else 0,
                "rounding_base": rounding_base,
                "continuous_audit": cont_audit,
                "categorical_audit": cat_audit,
            }
        )

    # Record any leftover records that were suppressed by leftover_action == "suppress"
    all_indices = set(range(len(df)))
    leftover_indices = np.array(sorted(all_indices - assigned_set), dtype=int)
    if len(leftover_indices) > 0:
        sub_df = df.iloc[leftover_indices]
        l_weights = all_unit_weights[leftover_indices] if all_unit_weights is not None else None
        cont_audit = {}
        for feat_name, spec in features_meta.items():
            if spec["type"] == "continuous":
                s_num = pd.to_numeric(sub_df[feat_name], errors="coerce")
                valid_mask = s_num.notna().to_numpy()
                vals = s_num[valid_mask].to_numpy(dtype=float)
                w_valid = l_weights[valid_mask] if l_weights is not None else None
                r_mean, r_std = _compute_weighted_mean_std(vals, w_valid)
                cont_audit[feat_name] = {
                    "raw_count": int(len(vals)),
                    "min_group_count": min_group_count,
                    "raw_mean": r_mean,
                    "exported_mean": np.nan,
                    "raw_std": r_std,
                    "exported_std": np.nan,
                    "status": "SUPPRESSED",
                }
        cat_audit = {}
        for feat_name, spec in features_meta.items():
            if spec["type"] == "categorical":
                cat_info = encoding_info["cat_mappings"][feat_name]
                mat = encoding_info["onehot_matrices"][feat_name][leftover_indices]
                raw_sums = (mat * l_weights[:, None]).sum(axis=0) if l_weights is not None else mat.sum(axis=0)
                raw_total = float(raw_sums.sum())
                raw_props = raw_sums / raw_total if raw_total > 0 else np.zeros_like(raw_sums)
                contributing_counts = (mat > 0).sum(axis=0)
                for col_idx, col_label in enumerate(cat_info["columns"]):
                    cat_audit[col_label] = {
                        "feature": feat_name,
                        "category": str(cat_info["categories"][col_idx]),
                        "unrounded_contributing_count": int(contributing_counts[col_idx]),
                        "raw_sum": float(raw_sums[col_idx]),
                        "raw_proportion": float(raw_props[col_idx]),
                        "exported_proportion": np.nan,
                        "min_cell_count": min_cell_count,
                        "status": "SUPPRESSED",
                    }
        cluster_audit_records.append(
            {
                "cluster_id": "SUPPRESSED_LEFTOVER",
                "status": "SUPPRESSED",
                "suppression_reason": (
                    f"Leftover records ({len(leftover_indices)}) < min_group_count ({min_group_count}) "
                    f"with leftover_action='suppress'"
                ),
                "indices": leftover_indices,
                "unrounded_count": int(len(leftover_indices)),
                "min_group_count": int(min_group_count),
                "min_cell_count": int(min_cell_count),
                "rounded_count": 0,
                "rounding_base": rounding_base,
                "continuous_audit": cont_audit,
                "categorical_audit": cat_audit,
            }
        )

    agg_df = pd.DataFrame(rows)

    if return_details:
        real_cat_mappings = {
            k: v for k, v in encoding_info["cat_mappings"].items() if k in features_meta
        }
        details = {
            "raw_df": df.copy(),
            "normalized_df": norm_df,
            "clusters": kept_clusters,
            "cluster_audit_records": cluster_audit_records,
            "features_meta": features_meta,
            "cat_mappings": real_cat_mappings,
            "max_values": encoding_info["max_values"],
            "output_rules": rules,
            "weight_col": weight_col if has_weights else None,
            "correlations": active_correlations,
        }
        return agg_df, details

    return agg_df


def build_unit_hierarchy_table(
    df: pd.DataFrame,
    group_id_cols: str or list,
    features_meta: dict,
    cat_mappings: dict,
    child_id_col: str or None = None,
    child_count_col: str = "individual_num",
    weight_col: str or None = None,
    hierarchy_counts: dict or None = None,
    level_name: str or None = None,
) -> tuple:
    """
    Step 2 helper: Merge individual unit records by `group_id_cols` (e.g. `[household_id, family_id]`
    or `household_id`) to form a unit-record family or household table prior to confidential clustering.
    Supports per-feature roll-up rules (`agg`: 'mean', 'sum', 'min', 'max', 'median') and
    conditional sub-group member counts (`hierarchy_counts`, e.g., `age < 18`).
    """
    if isinstance(group_id_cols, str):
        group_id_cols = [group_id_cols]

    work_df = df.copy()
    cat_cols_all = []
    for feat_name, cat_info in cat_mappings.items():
        series_str = work_df[feat_name].fillna("Missing").astype(str)
        for cat_val, col_label in zip(cat_info["categories"], cat_info["columns"]):
            work_df[col_label] = (series_str == str(cat_val)).astype(float)
            cat_cols_all.append(col_label)

    cont_cols = [f for f, spec in features_meta.items() if spec["type"] == "continuous"]
    mean_cont_cols = [f for f in cont_cols if features_meta[f].get("agg", "mean") == "mean"]
    other_cont_cols = [f for f in cont_cols if features_meta[f].get("agg", "mean") != "mean"]
    mean_cols = mean_cont_cols + cat_cols_all

    if weight_col and weight_col in work_df.columns:
        w_series = pd.to_numeric(work_df[weight_col], errors="coerce").fillna(1.0).clip(lower=0.0)
        weighted_work = work_df[group_id_cols].copy()
        weighted_work["__w"] = w_series.to_numpy(dtype=float)
        for col in mean_cols:
            weighted_work[col] = (
                pd.to_numeric(work_df[col], errors="coerce").fillna(0.0).to_numpy(dtype=float)
                * weighted_work["__w"]
            )
        grouped_sum = weighted_work.groupby(group_id_cols, as_index=False, sort=False).sum()
        w_denom = np.where(grouped_sum["__w"].to_numpy() > 0, grouped_sum["__w"].to_numpy(), 1.0)
        unit_level_df = grouped_sum[group_id_cols].copy()
        for col in mean_cols:
            unit_level_df[col] = grouped_sum[col].to_numpy(dtype=float) / w_denom
        unit_level_df[weight_col] = (
            work_df.groupby(group_id_cols, sort=False)[weight_col].mean().to_numpy(dtype=float)
        )
    else:
        grouped = work_df.groupby(group_id_cols, as_index=False, sort=False)
        if mean_cols:
            unit_level_df = grouped[mean_cols].mean()
        else:
            unit_level_df = work_df[group_id_cols].drop_duplicates().reset_index(drop=True)

    # Apply non-mean aggregations ('sum', 'min', 'max', 'median') for configured continuous features
    if other_cont_cols:
        grp_obj = work_df.groupby(group_id_cols, sort=False)
        for col in other_cont_cols:
            agg_mode = features_meta[col].get("agg", "mean")
            if agg_mode == "sum":
                unit_level_df[col] = grp_obj[col].sum().to_numpy(dtype=float)
            elif agg_mode == "min":
                unit_level_df[col] = grp_obj[col].min().to_numpy(dtype=float)
            elif agg_mode == "max":
                unit_level_df[col] = grp_obj[col].max().to_numpy(dtype=float)
            elif agg_mode == "median":
                unit_level_df[col] = grp_obj[col].median().to_numpy(dtype=float)

    if child_id_col is not None and child_id_col in work_df.columns:
        counts_series = work_df.groupby(group_id_cols, sort=False)[child_id_col].nunique().to_numpy(dtype=float)
    else:
        counts_series = work_df.groupby(group_id_cols, sort=False).size().to_numpy(dtype=float)

    unit_level_df[child_count_col] = counts_series

    level_features_meta = {k: dict(v) for k, v in features_meta.items()}
    level_features_meta[child_count_col] = {
        "type": "continuous",
        "weight": float(features_meta.get(child_count_col, {}).get("weight", 1.0)),
        "non_negative": True,
        "agg": "sum",
    }

    # Evaluate and aggregate conditional sub-group member counts (e.g., individual_under_18: "age < 18")
    if hierarchy_counts:
        for cnt_name, cnt_spec in hierarchy_counts.items():
            allowed_levels = cnt_spec.get("levels", ["family", "household"])
            if level_name is not None and level_name not in allowed_levels:
                continue
            cond_str = str(cnt_spec.get("condition", "")).strip()
            if not cond_str:
                continue
            if cond_str.lower() in ("all", "true", "1"):
                work_df[cnt_name] = 1.0
            else:
                work_df[cnt_name] = work_df.eval(cond_str).astype(float)
            unit_level_df[cnt_name] = (
                work_df.groupby(group_id_cols, sort=False)[cnt_name].sum().to_numpy(dtype=float)
            )
            level_features_meta[cnt_name] = {
                "type": "continuous",
                "weight": float(cnt_spec.get("weight", 1.0)),
                "non_negative": True,
                "agg": "sum",
                "is_hierarchy_count": True,
                "condition": cond_str,
            }

    return unit_level_df, level_features_meta


def aggregate_hierarchy(
    df: pd.DataFrame,
    config_or_path=None,
    return_metadata: bool = False,
    weight_df: pd.DataFrame or None = None,
) -> dict or tuple:
    """
    Execute Step 1 and Step 2 across all configured hierarchy levels
    (e.g., individual -> family -> household), optionally applying `weight_df`.

    Returns a dictionary of confidentialised aggregated DataFrames keyed by level name
    (e.g., `{"individual": ind_agg_df, "family": fam_agg_df, "household": hh_agg_df}`).
    """
    cfg = load_config(config_or_path)
    data_struct = resolve_data_structure(df, cfg)
    hierarchy = data_struct["hierarchy"]
    id_columns = data_struct["id_columns"]
    features_meta = data_struct["features"]
    hierarchy_counts = data_struct.get("hierarchy_counts", {})

    work_df, weight_info = resolve_weight_dataframe(df, weight_df, hierarchy, id_columns)
    weight_col = weight_info["weight_col"] if weight_info else None
    if weight_col and weight_col in features_meta:
        features_meta = {k: v for k, v in features_meta.items() if k != weight_col}

    agg_tables = {}
    meta_by_level = {}

    # 1. Individual level aggregation
    ind_agg, ind_details = aggregate_unit_records(
        work_df,
        cfg,
        features_meta=features_meta,
        weight_col=weight_col,
        return_details=True,
    )
    agg_tables["individual"] = ind_agg
    meta_by_level["individual"] = ind_details
    cat_mappings = ind_details["cat_mappings"]

    # 2. Family level aggregation (if present in hierarchy)
    if "family" in hierarchy:
        fam_col = id_columns.get("family", "family_id")
        if fam_col not in work_df.columns:
            raise ValueError(
                f"Hierarchy includes 'family', but ID column '{fam_col}' was not found in DataFrame."
            )
        hh_col = id_columns.get("household", "household_id")
        if "household" in hierarchy and hh_col in work_df.columns and hh_col != fam_col:
            fam_group_cols = [hh_col, fam_col]
        else:
            fam_group_cols = [fam_col]

        fam_unit_df, fam_features_meta = build_unit_hierarchy_table(
            df=work_df,
            group_id_cols=fam_group_cols,
            features_meta=features_meta,
            cat_mappings=cat_mappings,
            child_id_col=None,
            child_count_col="individual_num",
            weight_col=weight_col,
            hierarchy_counts=hierarchy_counts,
            level_name="family",
        )
        fam_agg, fam_details = aggregate_unit_records(
            fam_unit_df,
            cfg,
            features_meta=fam_features_meta,
            pre_encoded_categories=cat_mappings,
            weight_col=weight_col,
            return_details=True,
        )
        fam_details["unit_table"] = fam_unit_df
        agg_tables["family"] = fam_agg
        meta_by_level["family"] = fam_details

    # 3. Household level aggregation (if present in hierarchy)
    if "household" in hierarchy:
        hh_col = id_columns.get("household", "household_id")
        if hh_col not in work_df.columns:
            raise ValueError(
                f"Hierarchy includes 'household', but ID column '{hh_col}' was not found in DataFrame."
            )
        has_family = "family" in hierarchy and id_columns.get("family", "family_id") in work_df.columns
        child_col = id_columns.get("family") if has_family else None
        child_count_name = "family_num" if has_family else "individual_num"

        hh_unit_df, hh_features_meta = build_unit_hierarchy_table(
            df=work_df,
            group_id_cols=[hh_col],
            features_meta=features_meta,
            cat_mappings=cat_mappings,
            child_id_col=child_col,
            child_count_col=child_count_name,
            weight_col=weight_col,
            hierarchy_counts=hierarchy_counts,
            level_name="household",
        )
        hh_agg, hh_details = aggregate_unit_records(
            hh_unit_df,
            cfg,
            features_meta=hh_features_meta,
            pre_encoded_categories=cat_mappings,
            weight_col=weight_col,
            return_details=True,
        )
        hh_details["unit_table"] = hh_unit_df
        agg_tables["household"] = hh_agg
        meta_by_level["household"] = hh_details

    metadata = {
        "hierarchy": hierarchy,
        "id_columns": id_columns,
        "weight_info": weight_info,
        "features": features_meta,
        "hierarchy_counts": hierarchy_counts,
        "cat_mappings": cat_mappings,
        "output_rules": cfg["output_rules"],
        "levels": meta_by_level,
    }

    if return_metadata:
        return agg_tables, metadata
    return agg_tables
