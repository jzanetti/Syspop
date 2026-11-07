from copy import deepcopy
from pandas import DataFrame
from pandas.api.types import is_numeric_dtype, is_bool_dtype
from yaml import safe_load
from syspop.config import load_default_presets


DEFAULT_ID_COLUMNS = {
    "individual": "person_id",
    "family": "family_id",
    "household": "household_id",
}


def _normalize_rounding_base(val) -> int or None:
    """Return integer rounding base (>= 2), or None if rounding is switched off."""
    if val is None or val is False:
        return None
    if isinstance(val, str):
        s = val.strip().lower()
        if s in ("none", "null", "false", "off", "0", "1", ""):
            return None
        val = int(s)
    base = int(val)
    return base if base > 1 else None


def load_config(config_or_path=None) -> dict:
    """
    Load configuration from a YAML file path, a dictionary, or return default structure.
    """
    if config_or_path is None:
        raw_cfg = {}
    elif isinstance(config_or_path, str):
        with open(config_or_path, "r", encoding="utf-8") as fid:
            raw_cfg = safe_load(fid) or {}
    elif isinstance(config_or_path, dict):
        raw_cfg = deepcopy(config_or_path)
    else:
        raise TypeError("config_or_path must be a YAML file path (str), a dict, or None.")

    presets = load_default_presets()
    rules_cfg = raw_cfg.get("output_rules", {})
    preset_name = str(rules_cfg.get("preset", "idi")).lower()
    if preset_name not in presets:
        raise ValueError(
            f"Unknown output_rules preset '{preset_name}'. Choose from {list(presets.keys())}."
        )

    resolved_rules = deepcopy(presets[preset_name])
    resolved_rules["preset"] = preset_name
    resolved_rules.update(rules_cfg)

    # Normalize rounding_base (None means rounding is switched off)
    resolved_rules["rounding_base"] = _normalize_rounding_base(
        resolved_rules.get("rounding_base", 3)
    )
    resolved_rules["min_cell_count"] = int(resolved_rules.get("min_cell_count", 6))
    resolved_rules["min_group_count"] = max(
        int(resolved_rules.get("min_group_count", 20)),
        resolved_rules["min_cell_count"],
    )

    data_cfg = deepcopy(raw_cfg.get("data_structure", {}))
    synthesis_cfg = deepcopy(raw_cfg.get("synthesis", {}))

    return {
        "output_rules": resolved_rules,
        "data_structure": data_cfg,
        "synthesis": synthesis_cfg,
    }


def resolve_data_structure(df: DataFrame or None, cfg: dict) -> dict:
    """
    Automate discovery of hierarchy, ID columns, feature types (continuous vs categorical),
    and feature weights from the DataFrame and configuration.
    """
    data_cfg = deepcopy(cfg.get("data_structure", {}))

    # 1. Resolve ID columns mapping
    id_columns = deepcopy(DEFAULT_ID_COLUMNS)
    if "id_columns" in data_cfg and isinstance(data_cfg["id_columns"], dict):
        id_columns.update(data_cfg["id_columns"])

    if df is not None:
        col_lower_map = {c.lower().replace(" ", "_"): c for c in df.columns}
        for level, col_name in list(id_columns.items()):
            if col_name not in df.columns:
                norm_key = col_name.lower().replace(" ", "_")
                if norm_key in col_lower_map:
                    id_columns[level] = col_lower_map[norm_key]

    # 2. Resolve hierarchy order (lowest to highest: individual -> family -> household)
    if "hierarchy" in data_cfg and data_cfg["hierarchy"]:
        hierarchy = [str(h).lower() for h in data_cfg["hierarchy"]]
    elif df is not None:
        hierarchy = ["individual"]
        if id_columns.get("family") in df.columns:
            hierarchy.append("family")
        if id_columns.get("household") in df.columns:
            hierarchy.append("household")
    else:
        hierarchy = ["individual"]

    id_col_set = {
        id_columns[lvl]
        for lvl in hierarchy
        if lvl in id_columns and id_columns[lvl] is not None
    }
    for col_name in id_columns.values():
        if col_name:
            id_col_set.add(col_name)

    # 3. Resolve features, their types ("continuous" or "categorical"), and weights
    raw_features = data_cfg.get("features", None)
    raw_weights = data_cfg.get("weights", {}) or {}
    raw_types = data_cfg.get("feature_types", {}) or {}
    max_cat_unique = int(data_cfg.get("auto_categorical_max_unique", 10))

    features_meta = {}

    if isinstance(raw_features, dict):
        for feat_name, feat_spec in raw_features.items():
            f_agg = "mean"
            if isinstance(feat_spec, dict):
                f_type = feat_spec.get("type", raw_types.get(feat_name, None))
                f_weight = float(feat_spec.get("weight", raw_weights.get(feat_name, 1.0)))
                f_non_neg = feat_spec.get("non_negative", None)
                f_agg = feat_spec.get("agg", feat_spec.get("hierarchy_agg", "mean"))
            elif isinstance(feat_spec, str):
                f_type = feat_spec
                f_weight = float(raw_weights.get(feat_name, 1.0))
                f_non_neg = None
            elif isinstance(feat_spec, (int, float)):
                f_type = raw_types.get(feat_name, None)
                f_weight = float(feat_spec)
                f_non_neg = None
            else:
                f_type = None
                f_weight = 1.0
                f_non_neg = None

            if f_type is None and df is not None and feat_name in df.columns:
                f_type = _infer_feature_type(df[feat_name], max_cat_unique)
            elif f_type is None:
                f_type = "continuous"

            if f_non_neg is None and df is not None and feat_name in df.columns and f_type == "continuous":
                f_non_neg = bool((df[feat_name].dropna() >= 0).all())

            features_meta[feat_name] = {
                "type": _normalize_type_name(f_type),
                "weight": f_weight,
                "non_negative": True if f_non_neg is None else bool(f_non_neg),
                "agg": _normalize_agg_name(f_agg),
            }
    else:
        if isinstance(raw_features, list):
            candidate_cols = [c for c in raw_features if c not in id_col_set]
        elif df is not None:
            candidate_cols = [c for c in df.columns if c not in id_col_set]
        else:
            candidate_cols = [c for c in raw_types.keys() if c not in id_col_set]

        for feat_name in candidate_cols:
            f_type = raw_types.get(feat_name, None)
            if f_type is None and df is not None and feat_name in df.columns:
                f_type = _infer_feature_type(df[feat_name], max_cat_unique)
            elif f_type is None:
                f_type = "continuous"

            f_weight = float(raw_weights.get(feat_name, 1.0))
            f_non_neg = True
            if df is not None and feat_name in df.columns and _normalize_type_name(f_type) == "continuous":
                f_non_neg = bool((df[feat_name].dropna() >= 0).all())

            features_meta[feat_name] = {
                "type": _normalize_type_name(f_type),
                "weight": f_weight,
                "non_negative": f_non_neg,
                "agg": "mean",
            }

    # 4. Resolve optional hierarchy_counts (sub-group member counts when rolling up to family/household)
    raw_h_counts = data_cfg.get("hierarchy_counts", {}) or {}
    parent_levels = [lvl for lvl in hierarchy if lvl != "individual"]
    hierarchy_counts = {}
    if isinstance(raw_h_counts, dict):
        for cnt_name, cnt_spec in raw_h_counts.items():
            if isinstance(cnt_spec, str):
                cond_str = cnt_spec.strip()
                c_weight = 1.0
                c_levels = list(parent_levels)
            elif isinstance(cnt_spec, dict):
                cond_str = str(cnt_spec.get("condition", "")).strip()
                c_weight = float(cnt_spec.get("weight", 1.0))
                raw_lvls = cnt_spec.get("levels", parent_levels)
                c_levels = [str(l).lower() for l in raw_lvls] if isinstance(raw_lvls, list) else list(parent_levels)
            else:
                continue

            if cond_str:
                hierarchy_counts[cnt_name] = {
                    "condition": cond_str,
                    "weight": c_weight,
                    "levels": c_levels,
                }

    return {
        "hierarchy": hierarchy,
        "id_columns": id_columns,
        "features": features_meta,
        "hierarchy_counts": hierarchy_counts,
    }


def resolve_weight_dataframe(
    df: DataFrame,
    weight_df: DataFrame or None,
    hierarchy: list,
    id_columns: dict,
) -> tuple:
    """
    Validate and attach an optional parallel `weight_df` onto `df`.
    `weight_df` must contain a single weight column named consistently with one of the
    hierarchy levels (e.g., `individual_weight` / `person_weight`, `family_weight`, or `household_weight`).
    """
    if weight_df is None:
        return df, None

    if not isinstance(weight_df, DataFrame):
        raise TypeError("weight_df must be a pandas DataFrame or None.")

    id_col_values = {v for v in id_columns.values() if v is not None}
    weight_candidates = [c for c in weight_df.columns if c not in id_col_values]

    if len(weight_candidates) != 1:
        raise ValueError(
            f"weight_df must contain exactly one weight column (found {weight_candidates}). "
            f"Use a single weight column matching a hierarchy level, such as "
            f"'individual_weight' (or 'person_weight'), 'family_weight', or 'household_weight'."
        )

    weight_col = weight_candidates[0]
    col_norm = weight_col.strip().lower().replace(" ", "_")

    # Match weight_col name to one of the hierarchy levels
    matched_level = None
    level_prefixes = {
        "individual": ["individual", "person", "ind"],
        "family": ["family", "fam"],
        "household": ["household", "hh"],
    }
    for lvl, id_col in id_columns.items():
        if id_col:
            prefix = id_col.lower().replace("_id", "").strip()
            if prefix and prefix not in level_prefixes.get(lvl, []):
                level_prefixes.setdefault(lvl, []).append(prefix)

    for lvl, prefixes in level_prefixes.items():
        for p in prefixes:
            if col_norm in (f"{p}_weight", f"{p}_weights", f"{p}weight"):
                matched_level = lvl
                break
        if matched_level is not None:
            break

    if matched_level is None or matched_level not in hierarchy:
        raise ValueError(
            f"Weight column '{weight_col}' in weight_df must match one of the active hierarchy "
            f"levels {hierarchy} (e.g. 'individual_weight'/'person_weight', 'family_weight', or 'household_weight')."
        )

    work_df = df.copy()
    shared_id_cols = [c for c in weight_df.columns if c in id_col_values and c in work_df.columns]

    if shared_id_cols:
        w_dedup = weight_df[shared_id_cols + [weight_col]].groupby(shared_id_cols, as_index=False, sort=False)[weight_col].mean()
        work_df = work_df.merge(w_dedup, on=shared_id_cols, how="left")
    elif len(weight_df) == len(work_df):
        work_df[weight_col] = weight_df[weight_col].to_numpy(dtype=float)
    else:
        lvl_id_col = id_columns.get(matched_level)
        if lvl_id_col and lvl_id_col in work_df.columns and len(weight_df) == work_df[lvl_id_col].nunique():
            unique_ids = work_df[lvl_id_col].unique()
            id_to_w = dict(zip(unique_ids, weight_df[weight_col].to_numpy(dtype=float)))
            work_df[weight_col] = work_df[lvl_id_col].map(id_to_w)
        else:
            raise ValueError(
                f"Cannot align weight_df (len={len(weight_df)}) with df (len={len(work_df)}). "
                f"Include ID column(s) {list(id_columns.values())} in weight_df or match row count."
            )

    return work_df, {
        "weight_col": weight_col,
        "weight_level": matched_level,
    }


def _infer_feature_type(series, max_cat_unique: int = 10) -> str:
    """Automatically infer whether a pandas Series is continuous or categorical."""
    if is_bool_dtype(series) or not is_numeric_dtype(series):
        return "categorical"
    non_na = series.dropna()
    if len(non_na) == 0:
        return "continuous"
    is_int_like = bool((non_na % 1 == 0).all())
    if is_int_like and non_na.nunique() <= min(max_cat_unique, max(2, len(non_na) // 5)):
        return "categorical"
    return "continuous"


def _normalize_type_name(type_str: str) -> str:
    t = str(type_str).strip().lower()
    if t in ("continuous", "numeric", "float", "int", "number", "cont"):
        return "continuous"
    if t in ("categorical", "category", "cat", "string", "str", "bool"):
        return "categorical"
    raise ValueError(f"Unsupported feature type '{type_str}'. Use 'continuous' or 'categorical'.")


def _normalize_agg_name(agg_val) -> str:
    if agg_val is None:
        return "mean"
    a = str(agg_val).strip().lower()
    if a in ("mean", "avg", "average"):
        return "mean"
    if a in ("sum", "total"):
        return "sum"
    if a == "min":
        return "min"
    if a == "max":
        return "max"
    if a == "median":
        return "median"
    raise ValueError(
        f"Unsupported hierarchy aggregation '{agg_val}'. Use 'mean', 'sum', 'min', 'max', or 'median'."
    )
