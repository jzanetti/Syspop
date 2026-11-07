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
            if col_name and col_name not in df.columns:
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
            if isinstance(feat_spec, dict):
                f_type = feat_spec.get("type", raw_types.get(feat_name, None))
                f_weight = float(feat_spec.get("weight", raw_weights.get(feat_name, 1.0)))
                f_non_neg = feat_spec.get("non_negative", None)
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
            }
    else:
        if isinstance(raw_features, list):
            candidate_cols = [c for c in raw_features if c not in id_col_set]
        elif df is not None:
            candidate_cols = [c for c in df.columns if c not in id_col_set]
        else:
            candidate_cols = list(raw_types.keys())

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
            }

    return {
        "hierarchy": hierarchy,
        "id_columns": id_columns,
        "features": features_meta,
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
