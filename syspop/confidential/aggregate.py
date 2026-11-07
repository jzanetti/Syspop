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


def compute_pairwise_distances(df: pd.DataFrame, config_or_path=None) -> np.ndarray:
    """
    Compute the pairwise weighted squared distance matrix between all unit records in `df`
    following Section 1.1 of the Synthetic Data Production Algorithm.
    """
    cfg = load_config(config_or_path)
    data_struct = resolve_data_structure(df, cfg)
    _, X_weighted, _ = prepare_encoded_features(df, data_struct["features"])
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
                cur_v_a = _count_record_subcell_violations(
                    cur_a, onehot_matrices, min_cell_count, global_cat_counts
                )
                if cur_v_a == 0:
                    break

                without_a = np.delete(cur_a, pos_a)
                v_without_a = _count_record_subcell_violations(
                    without_a, onehot_matrices, min_cell_count, global_cat_counts
                )

                # Step A: If cluster A has > min_count records, move rec_a to a nearby compatible cluster B
                if len(cur_a) > min_count:
                    moved = False
                    for b_idx in other_order:
                        cur_b = clusters[b_idx]
                        v_b_before = _count_record_subcell_violations(
                            cur_b, onehot_matrices, min_cell_count, global_cat_counts
                        )
                        with_a_in_b = np.append(cur_b, rec_a)
                        v_b_after = _count_record_subcell_violations(
                            with_a_in_b, onehot_matrices, min_cell_count, global_cat_counts
                        )
                        if v_without_a + v_b_after < cur_v_a + v_b_before:
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
                    v_b_before = _count_record_subcell_violations(
                        cur_b, onehot_matrices, min_cell_count, global_cat_counts
                    )
                    for pos_b in range(len(cur_b)):
                        rec_b = cur_b[pos_b]
                        new_a = np.append(without_a, rec_b)
                        new_b = np.append(np.delete(cur_b, pos_b), rec_a)

                        v_a_after = _count_record_subcell_violations(
                            new_a, onehot_matrices, min_cell_count, global_cat_counts
                        )
                        v_b_after = _count_record_subcell_violations(
                            new_b, onehot_matrices, min_cell_count, global_cat_counts
                        )
                        if v_a_after + v_b_after < cur_v_a + v_b_before:
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
    or >= `min_cell_count` records (Option 1: 'reassign_or_merge').

    Strategy:
      1. Partition records by their exact categorical signature into homogeneous blocks
         of size >= `min_cell_count` (using nearest-neighbour on continuous features).
      2. Combine these homogeneous micro-blocks by nearest centroid distance into clusters
         of size >= `min_count`. Because every micro-block has >= `min_cell_count` identical
         categorical records, any sum of micro-blocks is guaranteed to have 0 or >= `min_cell_count`
         records in every category!
      3. Assign any leftover records from small categorical cells (< `min_cell_count`) to
         existing clusters that already have >= `min_cell_count` in each of that record's
         individual categories, or resolve via reassignment/merging.
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
        # For family/household level where categories are fractional proportions, use standard NN + reassignment
        base_clusters = _standard_nn_clusters(
            X_weighted, min_count, leftover_action, candidate_pool_size
        )
        return _resolve_small_subcells_by_reassignment(
            base_clusters, X_weighted, onehot_matrices, min_count, min_cell_count, global_cat_counts
        )

    # Build categorical signature per row
    combined_onehot = np.hstack(list(onehot_matrices.values()))
    # Group indices by identical one-hot row signature
    sig_groups = {}
    for row_idx in range(n_rows):
        sig = tuple(combined_onehot[row_idx].astype(int).tolist())
        sig_groups.setdefault(sig, []).append(row_idx)

    micro_blocks = []
    leftover_pool = []

    for sig, idx_list in sig_groups.items():
        idx_arr = np.array(idx_list, dtype=int)
        if len(idx_arr) < min_cell_count:
            leftover_pool.extend(idx_list)
            continue

        # Split large homogeneous cells into nearest-neighbour micro-blocks of size >= min_cell_count
        # If len(idx_arr) >= min_count, we can directly form full clusters of size min_count!
        u = idx_arr.copy()
        while len(u) >= min_cell_count:
            if len(u) < 2 * min_cell_count:
                micro_blocks.append(u.copy())
                u = np.array([], dtype=int)
                break
            target_sz = min_count if len(u) >= min_count + min_cell_count else min_cell_count
            X_u = X_weighted[u]
            dists = np.sum((X_u[0:1] - X_u) ** 2, axis=-1)[0]
            chosen_local = np.argsort(dists)[:target_sz]
            micro_blocks.append(u[chosen_local])
            mask = np.ones(len(u), dtype=bool)
            mask[chosen_local] = False
            u = u[mask]

        if len(u) > 0:
            leftover_pool.extend(u.tolist())

    # Combine micro-blocks into full clusters of size >= min_count
    ready_clusters = [b for b in micro_blocks if len(b) >= min_count]
    small_blocks = [b for b in micro_blocks if len(b) < min_count]

    while small_blocks:
        # Sort so smallest block is merged first
        small_blocks.sort(key=len)
        b_cur = small_blocks.pop(0)

        while len(b_cur) < min_count and small_blocks:
            c_cur = X_weighted[b_cur].mean(axis=0)
            block_centroids = np.array([X_weighted[b].mean(axis=0) for b in small_blocks])
            dists = np.sum((block_centroids - c_cur) ** 2, axis=1)
            nearest_idx = int(np.argmin(dists))
            b_cur = np.concatenate([b_cur, small_blocks.pop(nearest_idx)])

        if len(b_cur) >= min_count:
            ready_clusters.append(b_cur)
        elif ready_clusters:
            # Attach to nearest ready cluster
            c_cur = X_weighted[b_cur].mean(axis=0)
            ready_centroids = np.array([X_weighted[c].mean(axis=0) for c in ready_clusters])
            nearest_r = int(np.argmin(np.sum((ready_centroids - c_cur) ** 2, axis=1)))
            ready_clusters[nearest_r] = np.concatenate([ready_clusters[nearest_r], b_cur])
        else:
            leftover_pool.extend(b_cur.tolist())

    if not ready_clusters:
        base_clusters = _standard_nn_clusters(
            X_weighted, min_count, leftover_action, candidate_pool_size
        )
        return _resolve_small_subcells_by_reassignment(
            base_clusters, X_weighted, onehot_matrices, min_count, min_cell_count, global_cat_counts
        )

    # Handle leftover records from rare categorical cells
    if leftover_pool and str(leftover_action).strip().lower() == "assign_nearest":
        still_unassigned = []
        ready_centroids = np.array([X_weighted[c].mean(axis=0) for c in ready_clusters])
        for rec_idx in leftover_pool:
            # Find clusters where adding rec_idx creates ZERO small sub-cell violations
            compatible_clusters = [
                c_i
                for c_i, c_indices in enumerate(ready_clusters)
                if _record_fits_cluster_categories(rec_idx, c_indices, onehot_matrices, min_cell_count)
            ]
            if compatible_clusters:
                comp_centroids = ready_centroids[compatible_clusters]
                d = np.sum((comp_centroids - X_weighted[rec_idx]) ** 2, axis=1)
                chosen_c = compatible_clusters[int(np.argmin(d))]
                ready_clusters[chosen_c] = np.append(ready_clusters[chosen_c], rec_idx)
                ready_centroids[chosen_c] = X_weighted[ready_clusters[chosen_c]].mean(axis=0)
            else:
                still_unassigned.append(rec_idx)

        # If any records still unassigned, assign to nearest cluster and run reassignment/merge pass
        for rec_idx in still_unassigned:
            d = np.sum((ready_centroids - X_weighted[rec_idx]) ** 2, axis=1)
            chosen_c = int(np.argmin(d))
            ready_clusters[chosen_c] = np.append(ready_clusters[chosen_c], rec_idx)
            ready_centroids[chosen_c] = X_weighted[ready_clusters[chosen_c]].mean(axis=0)

    return _resolve_small_subcells_by_reassignment(
        ready_clusters, X_weighted, onehot_matrices, min_count, min_cell_count, global_cat_counts
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

    norm_df, X_weighted, encoding_info = prepare_encoded_features(
        df, features_meta, pre_encoded_categories=pre_encoded_categories
    )

    min_group_count = int(rules["min_group_count"])
    min_cell_count = int(rules.get("min_cell_count", 6))
    subcell_action = str(rules.get("subcell_action", "reassign_or_merge")).strip().lower()

    clusters = nearest_neighbour_grouping(
        X_weighted=X_weighted,
        min_count=min_group_count,
        leftover_action=rules.get("leftover_action", "assign_nearest"),
        onehot_matrices=encoding_info["onehot_matrices"],
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
            c_indices, encoding_info["onehot_matrices"], min_cell_count
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
        details = {
            "raw_df": df.copy(),
            "normalized_df": norm_df,
            "clusters": kept_clusters,
            "cluster_audit_records": cluster_audit_records,
            "features_meta": features_meta,
            "cat_mappings": encoding_info["cat_mappings"],
            "max_values": encoding_info["max_values"],
            "output_rules": rules,
            "weight_col": weight_col if has_weights else None,
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
