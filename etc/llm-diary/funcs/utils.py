from logging import INFO, Formatter, Logger, StreamHandler, getLogger

from funcs import LOCATIONS_CFG


def create_logger():
    """Creating logging information

    Returns:
        _type_: _description_
    """
    logger = getLogger(__name__)
    logger.setLevel(INFO)
    formatter = Formatter("%(message)s")
    console_handler = StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def check_locations(locations_to_check: list) -> bool:
    """Check if the location is allowed

    Args:
        locations_to_check (list): _description_
    """
    return set(locations_to_check).issubset(set(LOCATIONS_CFG.keys()))
