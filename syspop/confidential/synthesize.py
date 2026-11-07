import numpy as np
import pandas as pd
from syspop.confidential.config import load_config


HIERARCHY_COUNT_ALIASES = {
    "family": ["individual_num", "individual number", "individual_number", "ind_num"],
    "household_with_family": [
        "family_num",
        "family number",
        "family_number",
        "fam_num",
        "individual_num",
        "individual number",
    ],
    "household_no_family": [
        "individual_num",
        "individual number",
        "individual_number",
        "ind_num",
        "family_num",
        "family number",
    ],
}


def _match_column_case_insensitive(df: pd.DataFrame, candidates: list) -> str or None:
    col_map = {c.strip().lower(): c for c in df.columns}
    for cand in candidates:
        key = cand.strip().lower()
        if key in col_map:
            return col_map[key]
    return None


def infer_synthesis_schema(
    agg_tables: dict,
    cfg: dict,
    metadata: dict or None = None,
) -> dict:
    """
    Resolve continuous features, std columns, percentile columns, and categorical
    feature mappings from aggregated tables and configuration (even if Step 1 metadata
    is not provided, e.g., when running outside the Data Lab from exported CSVs).
    """
    if "individual" not in agg_tables:
        raise ValueError("agg_tables must contain at least an 'individual' DataFrame.")

    ind_df = agg_tables["individual"]
    data_cfg = cfg.get("data_structure", {})

    # 1. Hierarchy
    if metadata and "hierarchy" in metadata:
        hierarchy = list(metadata["hierarchy"])
    elif "hierarchy" in data_cfg and data_cfg["hierarchy"]:
        hierarchy = [str(h).lower() for h in data_cfg["hierarchy"]]
    else:
        hierarchy = ["individual"]
        if "family" in agg_tables:
            hierarchy.append("family")
        if "household" in agg_tables:
            hierarchy.append("household")

    # 2. ID columns
    id_columns = {
        "individual": "person_id",
        "family": "family_id",
        "household": "household_id",
    }
    if metadata and "id_columns" in metadata:
        id_columns.update(metadata["id_columns"])
    if "id_columns" in data_cfg and isinstance(data_cfg["id_columns"], dict):
        id_columns.update(data_cfg["id_columns"])

    # 3. Discover continuous and categorical features from metadata or aggregated columns
    count_col = _match_column_case_insensitive(ind_df, ["count", "value", "n"])
    if count_col is None:
        raise ValueError("Individual aggregated table must contain a 'count' column.")

    # Identify *_std columns in individual table
    std_col_map = {}
    for col in ind_df.columns:
        c_low = col.strip().lower()
        if c_low.endswith("_std") or c_low.endswith(" std"):
            base_prefix = col[:-4].strip().rstrip("_")
            # Find matching mean column (e.g., 'age' for 'Age std', or 'Ave income' for 'Income std')
            mean_col = _match_column_case_insensitive(
                ind_df,
                [base_prefix, f"ave {base_prefix}", f"avg {base_prefix}", f"mean {base_prefix}"],
            )
            if mean_col is not None:
                std_col_map[mean_col] = col

    if metadata and "features" in metadata and "cat_mappings" in metadata:
        features_meta = {}
        for f_name, f_spec in metadata["features"].items():
            if f_spec["type"] == "continuous":
                m_col = _match_column_case_insensitive(
                    ind_df, [f_name, f"ave {f_name}", f"avg {f_name}"]
                ) or f_name
                s_col = std_col_map.get(
                    m_col,
                    _match_column_case_insensitive(ind_df, [f"{f_name}_std", f"{f_name} std"]),
                )
                features_meta[f_name] = {
                    "type": "continuous",
                    "weight": float(f_spec.get("weight", 1.0)),
                    "non_negative": bool(f_spec.get("non_negative", True)),
                    "mean_col": m_col,
                    "std_col": s_col,
                }
            else:
                features_meta[f_name] = {
                    "type": "categorical",
                    "weight": float(f_spec.get("weight", 1.0)),
                }
        cat_mappings = metadata["cat_mappings"]
    else:
        # Auto-infer from aggregated table and config
        raw_features = data_cfg.get("features", {}) or {}
        raw_weights = data_cfg.get("weights", {}) or {}
        features_meta = {}
        cat_mappings = {}

        used_cols = {count_col}
        cluster_id_col = _match_column_case_insensitive(ind_df, ["cluster_id", "group_id"])
        if cluster_id_col:
            used_cols.add(cluster_id_col)
        for m_col, s_col in std_col_map.items():
            used_cols.add(m_col)
            used_cols.add(s_col)

            # Determine canonical feature name
            canon_name = m_col
            if m_col.lower().startswith("ave "):
                canon_name = m_col[4:].strip()
            weight = float(
                raw_weights.get(
                    canon_name,
                    raw_weights.get(
                        m_col,
                        raw_features.get(canon_name, {}).get("weight", 1.0)
                        if isinstance(raw_features, dict) and isinstance(raw_features.get(canon_name), dict)
                        else 1.0,
                    ),
                )
            )
            non_neg = bool((ind_df[m_col].dropna() >= 0).all())
            features_meta[canon_name] = {
                "type": "continuous",
                "weight": weight,
                "non_negative": non_neg,
                "mean_col": m_col,
                "std_col": s_col,
            }

        # Remaining columns in ind_df are categorical proportion columns
        rem_cols = [c for c in ind_df.columns if c not in used_cols]
        if isinstance(raw_features, dict):
            for f_name, f_spec in raw_features.items():
                if isinstance(f_spec, dict) and str(f_spec.get("type", "")).lower().startswith("cat"):
                    cats = f_spec.get("categories", f_spec.get("columns", None))
                    if cats:
                        matched_cols = [
                            _match_column_case_insensitive(ind_df, [ str(c), f"{f_name}_{c}" ])
                            for c in cats
                        ]
                        matched_cols = [c for c in matched_cols if c is not None]
                        if matched_cols:
                            cat_mappings[f_name] = {
                                "categories": [str(c) for c in cats],
                                "columns": matched_cols,
                            }
                            features_meta[f_name] = {
                                "type": "categorical",
                                "weight": float(f_spec.get("weight", raw_weights.get(f_name, 1.0))),
                            }
                            for mc in matched_cols:
                                if mc in rem_cols:
                                    rem_cols.remove(mc)

        if rem_cols:
            # Group remaining proportion columns into categorical variables (by configured name or prefix)
            configured_cat_names = set()
            if isinstance(raw_features, dict):
                for k, v in raw_features.items():
                    if k not in features_meta:
                        configured_cat_names.add(k)
            elif isinstance(raw_features, list):
                for k in raw_features:
                    if k not in features_meta:
                        configured_cat_names.add(k)

            prefix_groups = {}
            unprefixed = []
            for c in rem_cols:
                matched_pfx = None
                for cfg_name in sorted(configured_cat_names, key=len, reverse=True):
                    if c.startswith(f"{cfg_name}_"):
                        matched_pfx = cfg_name
                        cat_val = c[len(cfg_name) + 1 :]
                        prefix_groups.setdefault(matched_pfx, []).append((cat_val, c))
                        break
                if matched_pfx is None:
                    if "_" in c:
                        cand_pfx, cat_val = c.split("_", 1)
                        # Check if multiple columns share this prefix
                        if sum(1 for rc in rem_cols if rc.startswith(f"{cand_pfx}_")) >= 2:
                            prefix_groups.setdefault(cand_pfx, []).append((cat_val, c))
                        else:
                            unprefixed.append(c)
                    else:
                        unprefixed.append(c)

            for pfx, items in prefix_groups.items():
                f_weight = float(
                    raw_weights.get(
                        pfx,
                        raw_features.get(pfx, {}).get("weight", 1.0)
                        if isinstance(raw_features, dict) and isinstance(raw_features.get(pfx), dict)
                        else 1.0,
                    )
                )
                cat_mappings[pfx] = {
                    "categories": [it[0] for it in items],
                    "columns": [it[1] for it in items],
                }
                features_meta[pfx] = {
                    "type": "categorical",
                    "weight": f_weight,
                }

            if unprefixed:
                cat_feat_name = "sex" if {"male", "female"}.issubset({c.lower() for c in unprefixed}) else "category"
                if isinstance(raw_features, dict):
                    for k in raw_features:
                        if k not in features_meta:
                            cat_feat_name = k
                            break
                f_weight = float(
                    raw_weights.get(
                        cat_feat_name,
                        raw_features.get(cat_feat_name, {}).get("weight", 1.0)
                        if isinstance(raw_features, dict) and isinstance(raw_features.get(cat_feat_name), dict)
                        else 1.0,
                    )
                )
                cat_mappings[cat_feat_name] = {
                    "categories": list(unprefixed),
                    "columns": list(unprefixed),
                }
                features_meta[cat_feat_name] = {
                    "type": "categorical",
                    "weight": f_weight,
                }

    return {
        "hierarchy": hierarchy,
        "id_columns": id_columns,
        "features": features_meta,
        "cat_mappings": cat_mappings,
        "count_col": count_col,
    }


def _sample_continuous_column(
    row: pd.Series,
    feat_name: str,
    f_spec: dict,
    n_samples: int,
    rng: np.random.Generator,
    table_df: pd.DataFrame,
) -> np.ndarray:
    """Sample `n_samples` continuous values for a cluster row using normal distribution."""
    m_col = _match_column_case_insensitive(
        table_df, [f_spec.get("mean_col", feat_name), feat_name, f"ave {feat_name}", f"avg {feat_name}"]
    )
    s_col = _match_column_case_insensitive(
        table_df,
        [f_spec.get("std_col", f"{feat_name}_std"), f"{feat_name}_std", f"{feat_name} std"],
    )

    mean_val = float(row[m_col]) if m_col and pd.notna(row[m_col]) else 0.0
    std_val = float(row[s_col]) if s_col and pd.notna(row[s_col]) else 0.0
    std_val = max(0.0, std_val)

    if std_val > 0:
        samples = rng.normal(loc=mean_val, scale=std_val, size=n_samples)
    else:
        samples = np.full(n_samples, mean_val, dtype=float)

    if f_spec.get("non_negative", True):
        samples = np.maximum(0.0, samples)

    return samples


def recreate_individuals_from_agg(
    ind_agg_df: pd.DataFrame,
    schema: dict,
    cfg: dict,
    rng: np.random.Generator,
    target_total_count: int or None = None,
) -> pd.DataFrame:
    """
    Step 3.1: Recreate synthetic unit-record individuals from the confidentialised
    individual aggregated table.
    """
    count_col = _match_column_case_insensitive(ind_agg_df, [schema["count_col"], "count", "value"])
    raw_counts = ind_agg_df[count_col].fillna(0).to_numpy(dtype=float)

    if target_total_count is not None and target_total_count > 0 and raw_counts.sum() > 0:
        scaled = raw_counts * (float(target_total_count) / raw_counts.sum())
        int_counts = np.floor(scaled).astype(int)
        remainder = int(target_total_count - int_counts.sum())
        if remainder > 0:
            frac_order = np.argsort(-(scaled - int_counts))
            int_counts[frac_order[:remainder]] += 1
        cluster_counts = int_counts
    else:
        cluster_counts = np.round(raw_counts).astype(int)

    features_meta = schema["features"]
    cat_mappings = schema["cat_mappings"]

    chunks = []
    for row_idx, (_, row) in enumerate(ind_agg_df.iterrows()):
        n_g = int(cluster_counts[row_idx])
        if n_g <= 0:
            continue

        chunk = {}
        # Continuous features
        for feat_name, f_spec in features_meta.items():
            if f_spec["type"] != "continuous":
                continue
            chunk[feat_name] = _sample_continuous_column(
                row, feat_name, f_spec, n_g, rng, ind_agg_df
            )

        # Categorical features
        for feat_name, cat_info in cat_mappings.items():
            categories = cat_info["categories"]
            cols = [
                _match_column_case_insensitive(ind_agg_df, [c]) or c
                for c in cat_info["columns"]
            ]
            probs = np.array([max(0.0, float(row[c])) if c in row and pd.notna(row[c]) else 0.0 for c in cols])
            p_sum = probs.sum()
            if p_sum > 0:
                probs = probs / p_sum
            else:
                probs = np.ones(len(categories), dtype=float) / len(categories)

            chosen = rng.choice(categories, size=n_g, p=probs)
            chunk[feat_name] = chosen
            for cat_val, col_label in zip(categories, cat_info["columns"]):
                chunk[f"__onehot_{col_label}"] = (chosen == cat_val).astype(float)

        chunks.append(pd.DataFrame(chunk))

    if not chunks:
        return pd.DataFrame()

    ind_records = pd.concat(chunks, ignore_index=True)
    ind_id_col = schema["id_columns"].get("individual", "person_id")
    ind_records.insert(0, ind_id_col, np.arange(1, len(ind_records) + 1, dtype=int))
    return ind_records


def _generate_parent_targets(
    parent_agg_df: pd.DataFrame,
    child_count_aliases: list,
    schema: dict,
    cfg: dict,
    rng: np.random.Generator,
    target_parent_count: int or None = None,
) -> tuple:
    """
    Generate synthetic parent unit targets (e.g., families or households) from a
    confidentialised parent-level aggregated table.
    """
    count_col = _match_column_case_insensitive(parent_agg_df, [schema["count_col"], "count", "value"])
    raw_counts = parent_agg_df[count_col].fillna(0).to_numpy(dtype=float)

    if target_parent_count is not None and target_parent_count > 0 and raw_counts.sum() > 0:
        scaled = raw_counts * (float(target_parent_count) / raw_counts.sum())
        int_counts = np.floor(scaled).astype(int)
        rem = int(target_parent_count - int_counts.sum())
        if rem > 0:
            frac_order = np.argsort(-(scaled - int_counts))
            int_counts[frac_order[:rem]] += 1
        cluster_counts = int_counts
    else:
        cluster_counts = np.round(raw_counts).astype(int)

    child_num_col = _match_column_case_insensitive(parent_agg_df, child_count_aliases)
    if child_num_col is None:
        raise ValueError(
            f"Could not find child count column (expected one of {child_count_aliases}) "
            f"in aggregated table columns {list(parent_agg_df.columns)}."
        )
    child_num_std_col = _match_column_case_insensitive(
        parent_agg_df, [f"{child_num_col}_std", f"{child_num_col} std"]
    )

    features_meta = schema["features"]
    cat_mappings = schema["cat_mappings"]

    parent_chunks = []
    for row_idx, (_, row) in enumerate(parent_agg_df.iterrows()):
        n_p = int(cluster_counts[row_idx])
        if n_p <= 0:
            continue

        chunk = {}
        for feat_name, f_spec in features_meta.items():
            if f_spec["type"] != "continuous":
                continue
            chunk[feat_name] = _sample_continuous_column(
                row, feat_name, f_spec, n_p, rng, parent_agg_df
            )

        for feat_name, cat_info in cat_mappings.items():
            cols = [
                _match_column_case_insensitive(parent_agg_df, [c]) or c
                for c in cat_info["columns"]
            ]
            probs = np.array([max(0.0, float(row[c])) if c in row and pd.notna(row[c]) else 0.0 for c in cols])
            p_sum = probs.sum()
            probs = probs / p_sum if p_sum > 0 else np.ones(len(cols)) / len(cols)
            for col_idx, col_label in enumerate(cat_info["columns"]):
                chunk[f"__onehot_{col_label}"] = np.full(n_p, probs[col_idx], dtype=float)

        # Sample or assign number of children per parent unit
        mean_c = max(1.0, float(row[child_num_col]))
        std_c = float(row[child_num_std_col]) if child_num_std_col and pd.notna(row[child_num_std_col]) else 0.0
        if std_c > 0:
            raw_sizes = np.maximum(1.0, np.round(rng.normal(mean_c, std_c, size=n_p)))
        else:
            base_sz = int(np.floor(mean_c))
            frac = mean_c - base_sz
            n_up = int(round(frac * n_p))
            raw_sizes = np.full(n_p, max(1, base_sz), dtype=int)
            if n_up > 0:
                raw_sizes[:n_up] += 1
                rng.shuffle(raw_sizes)

        chunk["__child_count"] = raw_sizes.astype(int)
        parent_chunks.append(pd.DataFrame(chunk))

    if not parent_chunks:
        return pd.DataFrame(), child_num_col

    return pd.concat(parent_chunks, ignore_index=True), child_num_col


def _build_matching_vectors(
    df: pd.DataFrame,
    schema: dict,
    max_values: dict,
) -> np.ndarray:
    """
    Construct weighted normalized feature vectors for greedy nearest-neighbour matching
    between child units and parent targets.
    """
    features_meta = schema["features"]
    cat_mappings = schema["cat_mappings"]
    n_rows = len(df)
    cols = []

    for feat_name, f_spec in features_meta.items():
        w = float(f_spec.get("weight", 1.0))
        if f_spec["type"] == "continuous":
            max_v = max_values.get(feat_name, 1.0)
            norm_v = df[feat_name].to_numpy(dtype=float) / max_v
            cols.append(norm_v[:, None] * np.sqrt(w))
        elif f_spec["type"] == "categorical":
            cat_info = cat_mappings[feat_name]
            onehot_cols = [f"__onehot_{c}" for c in cat_info["columns"]]
            mat = df[onehot_cols].to_numpy(dtype=float)
            cols.append(mat * np.sqrt(w * 0.5))

    if not cols:
        return np.zeros((n_rows, 1), dtype=float)
    return np.hstack(cols)


def assign_children_to_parents_greedy(
    children_df: pd.DataFrame,
    parents_df: pd.DataFrame,
    schema: dict,
    parent_id_col: str,
) -> tuple:
    """
    Assign every child unit in `children_df` to a parent unit in `parents_df` (where
    `parents_df['__child_count']` sums to `len(children_df)`) using greedy nearest-neighbour
    matching in weighted normalized feature space to minimize parent-level residual error.
    """
    features_meta = schema["features"]
    cat_mappings = schema["cat_mappings"]

    max_values = {}
    for feat_name, f_spec in features_meta.items():
        if f_spec["type"] == "continuous":
            m_child = float(np.max(np.abs(children_df[feat_name].to_numpy(dtype=float)))) if len(children_df) else 1.0
            m_parent = float(np.max(np.abs(parents_df[feat_name].to_numpy(dtype=float)))) if len(parents_df) else 1.0
            max_v = max(m_child, m_parent)
            max_values[feat_name] = max_v if max_v > 0 else 1.0

    X_child = _build_matching_vectors(children_df, schema, max_values)
    X_parent_target = _build_matching_vectors(parents_df, schema, max_values)

    n_children = len(children_df)
    n_parents = len(parents_df)
    child_counts = parents_df["__child_count"].to_numpy(dtype=int)

    available_mask = np.ones(n_children, dtype=bool)
    available_indices = np.arange(n_children, dtype=int)
    assigned_parent_ids = np.zeros(n_children, dtype=int)

    parent_running_sums = np.zeros_like(X_parent_target)

    # Assign member by member across parents (round-robin by slot depth r = 1..max_m)
    # so no single parent monopolizes all best matches before other parents get their first member.
    max_members = int(np.max(child_counts)) if n_parents > 0 else 0
    for slot_r in range(1, max_members + 1):
        for p_idx in range(n_parents):
            if child_counts[p_idx] < slot_r:
                continue
            avail_pos = np.flatnonzero(available_mask)
            if len(avail_pos) == 0:
                break

            # Desired child vector to bring running average (s + x_i) / r as close as possible to target t_p:
            # d_r = r * t_p - s
            desired_vec = slot_r * X_parent_target[p_idx] - parent_running_sums[p_idx]
            X_avail = X_child[avail_pos]
            dists = np.sum((X_avail - desired_vec) ** 2, axis=1)
            best_local = int(np.argmin(dists))
            chosen_child_idx = int(avail_pos[best_local])

            available_mask[chosen_child_idx] = False
            assigned_parent_ids[chosen_child_idx] = p_idx + 1
            parent_running_sums[p_idx] += X_child[chosen_child_idx]

    linked_children = children_df.copy()
    linked_children[parent_id_col] = assigned_parent_ids

    # Build realized parent summary table & residual diagnostics
    cont_cols = [f for f, s in features_meta.items() if s["type"] == "continuous"]
    onehot_cols = [
        f"__onehot_{col_label}"
        for cat_info in cat_mappings.values()
        for col_label in cat_info["columns"]
    ]
    agg_cols = cont_cols + onehot_cols

    realized_parents = linked_children.groupby(parent_id_col, as_index=False)[agg_cols].mean()
    realized_parents["__child_count"] = linked_children.groupby(parent_id_col).size().to_numpy(dtype=int)

    # Compute residual error summary (target - realized)
    residuals = {}
    for col in cont_cols:
        residuals[f"{col}_mae"] = float(
            np.mean(np.abs(parents_df[col].to_numpy() - realized_parents[col].to_numpy()))
        )
    for cat_info in cat_mappings.values():
        for col_label in cat_info["columns"]:
            oh_col = f"__onehot_{col_label}"
            residuals[f"{col_label}_mae"] = float(
                np.mean(np.abs(parents_df[oh_col].to_numpy() - realized_parents[oh_col].to_numpy()))
            )

    return linked_children, realized_parents, residuals


def create_synthetic_dataset(
    agg_tables: dict or pd.DataFrame,
    config_or_path=None,
    metadata: dict or None = None,
    return_diagnostics: bool = False,
) -> pd.DataFrame or tuple:
    """
    Step 3 of the Synthetic Data Production Algorithm:
    Recreate the synthetic unit-record dataset outside the Data Lab from the
    released confidentialised aggregated tables (Individual, and optionally Family
    and Household tables) while minimizing residual errors across the hierarchy.
    """
    if isinstance(agg_tables, pd.DataFrame):
        agg_tables = {"individual": agg_tables}

    cfg = load_config(config_or_path)
    schema = infer_synthesis_schema(agg_tables, cfg, metadata=metadata)
    hierarchy = schema["hierarchy"]
    id_columns = schema["id_columns"]

    seed = cfg.get("synthesis", {}).get(
        "random_seed", cfg.get("output_rules", {}).get("random_seed", None)
    )
    rng = np.random.default_rng(seed)

    has_family = "family" in hierarchy and "family" in agg_tables and len(agg_tables["family"]) > 0
    has_household = "household" in hierarchy and "household" in agg_tables and len(agg_tables["household"]) > 0

    diagnostics = {"residuals": {}}

    # 1. If household and family both exist, first generate household targets so we know total required families
    hh_targets_df = None
    total_families_needed = None
    if has_household:
        hh_aliases = (
            HIERARCHY_COUNT_ALIASES["household_with_family"]
            if has_family
            else HIERARCHY_COUNT_ALIASES["household_no_family"]
        )
        hh_targets_df, _ = _generate_parent_targets(
            agg_tables["household"], hh_aliases, schema, cfg, rng
        )
        if has_family and len(hh_targets_df) > 0:
            reconcile_mode = str(cfg.get("synthesis", {}).get("reconcile_counts", "auto")).lower()
            if reconcile_mode == "auto":
                total_families_needed = int(hh_targets_df["__child_count"].sum())

    # 2. If family exists, generate family targets so we know total required individuals
    fam_targets_df = None
    total_individuals_needed = None
    if has_family:
        fam_targets_df, _ = _generate_parent_targets(
            agg_tables["family"],
            HIERARCHY_COUNT_ALIASES["family"],
            schema,
            cfg,
            rng,
            target_parent_count=total_families_needed,
        )
        if len(fam_targets_df) > 0:
            reconcile_mode = str(cfg.get("synthesis", {}).get("reconcile_counts", "auto")).lower()
            if reconcile_mode == "auto":
                total_individuals_needed = int(fam_targets_df["__child_count"].sum())
    elif has_household and hh_targets_df is not None and len(hh_targets_df) > 0:
        reconcile_mode = str(cfg.get("synthesis", {}).get("reconcile_counts", "auto")).lower()
        if reconcile_mode == "auto":
            total_individuals_needed = int(hh_targets_df["__child_count"].sum())

    # 3. Recreate synthetic unit-record individuals
    ind_df = recreate_individuals_from_agg(
        agg_tables["individual"],
        schema,
        cfg,
        rng,
        target_total_count=total_individuals_needed,
    )

    if len(ind_df) == 0:
        if return_diagnostics:
            return ind_df, diagnostics
        return ind_df

    # 4. Link individuals -> families (if family level is active)
    if has_family and fam_targets_df is not None and len(fam_targets_df) > 0:
        fam_id_col = id_columns.get("family", "family_id")
        # Ensure exact count match if reconcile_counts was disabled
        diff = len(ind_df) - int(fam_targets_df["__child_count"].sum())
        if diff != 0:
            ind_df = recreate_individuals_from_agg(
                agg_tables["individual"],
                schema,
                cfg,
                rng,
                target_total_count=int(fam_targets_df["__child_count"].sum()),
            )

        ind_df, realized_families_df, fam_residuals = assign_children_to_parents_greedy(
            children_df=ind_df,
            parents_df=fam_targets_df,
            schema=schema,
            parent_id_col=fam_id_col,
        )
        diagnostics["residuals"]["family"] = fam_residuals

        # 5. Link families -> households (if household level is also active)
        if has_household and hh_targets_df is not None and len(hh_targets_df) > 0:
            hh_id_col = id_columns.get("household", "household_id")
            linked_families, realized_hh_df, hh_residuals = assign_children_to_parents_greedy(
                children_df=realized_families_df,
                parents_df=hh_targets_df,
                schema=schema,
                parent_id_col=hh_id_col,
            )
            diagnostics["residuals"]["household"] = hh_residuals
            fam_to_hh = dict(zip(linked_families[fam_id_col], linked_families[hh_id_col]))
            ind_df[hh_id_col] = ind_df[fam_id_col].map(fam_to_hh)

    elif has_household and hh_targets_df is not None and len(hh_targets_df) > 0:
        # Link individuals directly -> households (when there is no family level)
        hh_id_col = id_columns.get("household", "household_id")
        diff = len(ind_df) - int(hh_targets_df["__child_count"].sum())
        if diff != 0:
            ind_df = recreate_individuals_from_agg(
                agg_tables["individual"],
                schema,
                cfg,
                rng,
                target_total_count=int(hh_targets_df["__child_count"].sum()),
            )
        ind_df, _, hh_residuals = assign_children_to_parents_greedy(
            children_df=ind_df,
            parents_df=hh_targets_df,
            schema=schema,
            parent_id_col=hh_id_col,
        )
        diagnostics["residuals"]["household"] = hh_residuals

    # Clean up internal helper columns (__onehot_*) and order ID columns first
    helper_cols = [c for c in ind_df.columns if c.startswith("__")]
    ind_df = ind_df.drop(columns=helper_cols)

    ordered_ids = []
    for lvl in reversed(hierarchy):
        col = id_columns.get(lvl)
        if col and col in ind_df.columns and col not in ordered_ids:
            ordered_ids.append(col)
    other_cols = [c for c in ind_df.columns if c not in ordered_ids]
    ind_df = ind_df[ordered_ids + other_cols].sort_values(by=ordered_ids).reset_index(drop=True)

    # Re-number family_id and person_id sequentially in sorted order so IDs are clean (1, 2, 3, ...)
    fam_id_col = id_columns.get("family", "family_id")
    hh_id_col = id_columns.get("household", "household_id")
    if fam_id_col in ind_df.columns:
        if hh_id_col in ind_df.columns and hh_id_col != fam_id_col:
            ind_df[fam_id_col] = (
                ind_df.groupby([hh_id_col, fam_id_col], sort=False).ngroup() + 1
            )
        else:
            ind_df[fam_id_col] = ind_df.groupby(fam_id_col, sort=False).ngroup() + 1

    ind_id_col = id_columns.get("individual", "person_id")
    if ind_id_col in ind_df.columns:
        ind_df[ind_id_col] = np.arange(1, len(ind_df) + 1, dtype=int)

    if return_diagnostics:
        return ind_df, diagnostics
    return ind_df
