from logging import INFO, FileHandler, Formatter, StreamHandler, getLogger

from funcs import LOCATIONS_CFG


def create_logger(logger_path: str or None = None):
    """Creating logging information

    Returns:
        _type_: _description_
    """
    logger = getLogger(__name__)
    logger.setLevel(INFO)
    formatter = Formatter("%(message)s")
    if logger_path is not None:
        console_handler = FileHandler(logger_path)
    else:
        console_handler = StreamHandler(logger_path)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def check_locations(locations_to_check: list) -> bool:
    """Check if the location is allowed

    Args:
        locations_to_check (list): _description_
    """
    return set(locations_to_check).issubset(set(LOCATIONS_CFG.keys()))
