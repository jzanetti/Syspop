from os.path import dirname, join
from yaml import safe_load


def load_default_rules() -> dict:
    """Load the built-in default output_rules configuration."""
    cfg_path = join(dirname(__file__), "default_rules.yml")
    with open(cfg_path, "r", encoding="utf-8") as fid:
        return safe_load(fid)["output_rules"]
