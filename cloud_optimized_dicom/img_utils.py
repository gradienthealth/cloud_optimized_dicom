import io
import logging

import pydicom3
import pydicom3.encaps
import pydicom3.uid
from openjpeg.utils import _get_format as get_j2k_format
from openjpeg.utils import get_parameters as get_j2k_parameters
from pydicom3.pixel_data_handlers.pylibjpeg_handler import SUPPORTED_TRANSFER_SYNTAXES

logger = logging.getLogger(__name__)


def _correct_bit_stored_jp2k(ds):
    """Get JPEG 2000 metadata and fix the dicom header."""
    stream = io.BytesIO(
        pydicom3.encaps._decode_data_sequence(ds.PixelData)[0]
    )  # takes the first fragment and try to read jpeg 2000 header
    j2k_format = get_j2k_format(stream)
    meta = get_j2k_parameters(stream, j2k_format)
    if ds.BitsStored != meta["precision"]:
        ds.BitsStored = meta["precision"]
        logger.debug("BitsStored changed to %s", meta["precision"])


def decode_pixel_data(ds: pydicom3.Dataset):
    """This function calls the convert_pixel_data function with determined handler
    based on the transfer syntax, also correct the dicom header if the metadata
    in the pixel data is different from the dicom header"""
    # compressed transfer syntax
    if ds.file_meta.TransferSyntaxUID in SUPPORTED_TRANSFER_SYNTAXES:
        if ds.file_meta.TransferSyntaxUID in pydicom3.uid.JPEG2000TransferSyntaxes:
            try:
                _correct_bit_stored_jp2k(ds)
            except Exception as e:
                raise ValueError(
                    "Failed to modify BitsStored according to Jp2k header"
                ) from e
        ds.convert_pixel_data(handler_name="pylibjpeg")
    # uncompressed transfer syntax
    elif ds.file_meta.TransferSyntaxUID in [
        pydicom3.uid.ExplicitVRLittleEndian,
        pydicom3.uid.ImplicitVRLittleEndian,
    ]:
        ds.convert_pixel_data(handler_name="numpy")
    else:
        if ds.file_meta.TransferSyntaxUID.is_compressed:
            logger.warning(
                "When decoding compressed pixel data to numpy array, unable to find a handler. This is untested and unexpected."
            )
        ds.convert_pixel_data()
