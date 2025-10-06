import logging

import pydicom3

logger = logging.getLogger("cloud_optimized_dicom")
logger.addHandler(logging.NullHandler())

debugging: bool


def debug(debug_on: bool = True, default_handler: bool = True) -> None:
    """Turn on/off debugging/logging for cloud_optimized_dicom.

    When debugging is on, details about the operations are logged to the 'cloud_optimized_dicom' logger using Python's
    :mod:`logging`
    module.

    Parameters
    ----------
    debug_on : bool, optional
        If ``True`` (default) then turn on debugging, ``False`` to turn off.
    default_handler : bool, optional
        If ``True`` (default) then use :class:`logging.StreamHandler` as the
        handler for log messages.
    """
    global logger, debugging

    if default_handler:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    if debug_on:
        logger.setLevel(logging.DEBUG)
        debugging = True
    else:
        logger.setLevel(logging.WARNING)
        debugging = False


def get_child_logger(name: str):
    return logger.getChild(name)


# by default, we leave the NullHandler in place and set logging to WARNING
debug(False, False)

# To maximize compatibility we set pydicom3.config.convert_wrong_length_to_UN = True by default
# Pydicom's default behavior (False) is to raise an error when attempting to read a DICOM file with a weird/bad private tag
# The design philosophy of COD is to "ingest everything we can", which includes such technically invalid DICOM files
# If a user wants to be more strict, they can set pydicom3.config.convert_wrong_length_to_UN = False
pydicom3.config.convert_wrong_length_to_UN = True
