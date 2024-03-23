from logging import INFO, Formatter, Logger, StreamHandler, getLogger


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
