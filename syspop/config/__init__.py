from os.path import dirname, join
from yaml import safe_load


def load_default_presets() -> dict:
    """Load the built-in Stats NZ Microdata Output Guide rule presets."""
    cfg_path = join(dirname(__file__), "default_rules.yml")
    with open(cfg_path, "r", encoding="utf-8") as fid:
        return safe_load(fid)["presets"]
