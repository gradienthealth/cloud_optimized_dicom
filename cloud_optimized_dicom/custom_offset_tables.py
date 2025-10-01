from typing import Generator, Tuple

import pydicom3
import pydicom3.errors
import pydicom3.filebase
import pydicom3.tag

from cloud_optimized_dicom.config import logger

ITEM_TAG_PLUS_ITEM_LENGTH_SIZE = 8
FRAGMENT_PADDING = 8
BOT_PER_ELEMENT_SIZE = 4


def _generate_pixel_data_fragment_extended(
    fp: pydicom3.filebase.DicomFileLike,
) -> Generator[Tuple[bytes, int], None, None]:
    """
    Based on PyDICOM generate_pixel_data_fragment

    Args:
        fp (filebase.DicomFileLike): The encoded (7FE0,0010) *Pixel Data* element value, positioned at the
            start of the item tag for the first item after the Basic Offset Table
            item. ``fp.is_little_endian`` should be set to ``True``.

    Yields:
        Generator[bytes, int]: A pixel data fragment,
                               byte position

    Raises:
        ValueError: If the data contains an item with an undefined length or an unknown tag.

    Notes:
        The encoding of the data shall be little endian.

    References:
        DICOM Standard Part 5, :dcm:`Annex A.4 <part05/sect_A.4.html>`
    """
    if not fp.is_little_endian:
        raise ValueError("'fp.is_little_endian' must be True")

    # We should be positioned at the start of the Item Tag for the first
    # fragment after the Basic Offset Table
    while True:
        try:
            tag = pydicom3.tag.Tag(fp.read_tag())
        except EOFError:
            break

        if tag == 0xFFFEE000:
            # Item
            length = fp.read_UL()
            if length == 0xFFFFFFFF:
                raise ValueError(
                    f"Undefined item length at offset {fp.tell() - 4} when "
                    "parsing the encapsulated pixel data fragments"
                )
            cur_pos = fp.tell()
            yield fp.read(length), cur_pos
        elif tag == 0xFFFEE0DD:
            # Sequence Delimiter
            # Behave nicely and rewind back to the end of the items
            fp.seek(-4, 1)
            break
        else:
            raise ValueError(
                f"Unexpected tag '{tag}' at offset {fp.tell() - 4} when "
                "parsing the encapsulated pixel data fragment items"
            )


def _get_offsets_for_encapsulated_pixeldata(
    pixel_data_offset: int,
    pixel_data: bytes,
) -> Generator[Tuple[int, int, int], None, None]:
    """
    Generates offsets for encapsulated pixel data.

    Parameters:
        pixel_data_offset (int): The offset of the pixel data in the file.
        pixel_data (bytes): The pixel data as a byte string.

    Yields:
        Tuple[int, int, int]: A tuple containing the offset,
                              start position,
                              and frame length for each frame.

    Notes:
        This function assumes that the pixel data is in little-endian byte order.
        It uses the `_generate_pixel_data_fragment_extended` function to iterate over the
        fragments of the pixel data and calculates the offsets and file positions accordingly.
    """
    dicom_bytes_io = pydicom3.filebase.DicomBytesIO(pixel_data)
    dicom_bytes_io.is_little_endian = True

    fragment_count = 0
    for fragment, position in _generate_pixel_data_fragment_extended(dicom_bytes_io):
        fragment_size = len(fragment)

        if fragment_count == 0:
            # Basic Offset Table length
            basic_offset_table_length = fragment_size

            if basic_offset_table_length:
                num_of_frames = int(basic_offset_table_length / BOT_PER_ELEMENT_SIZE)

                for i in range(num_of_frames):
                    offset = int.from_bytes(
                        fragment[
                            i * BOT_PER_ELEMENT_SIZE : (i + 1) * BOT_PER_ELEMENT_SIZE
                        ],
                        byteorder="little",
                    )

                    file_pos = (
                        pixel_data_offset
                        + basic_offset_table_length
                        + (ITEM_TAG_PLUS_ITEM_LENGTH_SIZE * 2)
                        + offset
                    )

                    next_offset = (
                        int.from_bytes(
                            fragment[
                                (i + 1)
                                * BOT_PER_ELEMENT_SIZE : (i + 2)
                                * BOT_PER_ELEMENT_SIZE
                            ],
                            byteorder="little",
                        )
                        if i + 1 < num_of_frames
                        else len(pixel_data)
                        - basic_offset_table_length
                        - ITEM_TAG_PLUS_ITEM_LENGTH_SIZE
                    )
                    length = next_offset - offset - FRAGMENT_PADDING

                    yield offset, file_pos, length

                return

        elif fragment_count == 1:
            # Init offset is BOT length + BOT size + Item size
            init_offset_bytes = basic_offset_table_length + (
                ITEM_TAG_PLUS_ITEM_LENGTH_SIZE * 2
            )
            file_pos = pixel_data_offset + init_offset_bytes
            yield 0, file_pos, fragment_size

        elif fragment_count > 1 and fragment_size:
            offset = position - init_offset_bytes
            file_pos = pixel_data_offset + position
            yield offset, file_pos, fragment_size

        fragment_count += 1


def _get_offsets_for_uncompressed_pixeldata(
    pixel_data_offset: int,
    pixel_data_element: pydicom3.DataElement,
    num_of_frames: int,
) -> Generator[Tuple[int, int, int], None, None]:
    """
    Generates offsets for uncompressed pixel data.

    Args:
        pixel_data_offset (int): The offset of the pixel data.
        pixel_data_element (DataElement): The data element containing the pixel data.
        num_of_frames (int): The number of frames in the pixel data.

    Yields:
        Tuple[int, int, int]: A tuple containing the offset,
                              start position,
                              and frame length for each frame.

    Notes:
        The function calculates the start position by adding the pixel data offset and the size of the item tag plus item length.
        The frame length is calculated by dividing the length of the pixel data element value by the number of frames.
        The function then yields a tuple containing the offset, start position, and frame length for each frame.
    """
    start = pixel_data_offset + ITEM_TAG_PLUS_ITEM_LENGTH_SIZE
    frame_len = int(len(pixel_data_element.value) / num_of_frames)
    for i in range(num_of_frames):
        offset = i * frame_len
        yield offset, offset + start, frame_len


def _generate_pixel_data_fragment_offsets(
    dataset: pydicom3.Dataset,
) -> Generator[Tuple[int, int, int], None, None]:
    """
    DICOM Standard :
    https://dicom.nema.org/dicom/2013/output/chtml/part05/sect_A.4.html
    Calculations based on Table 4.1

    Pixel data - 0x7FE0, 0x0010

    Basic Offset Table(BOT)(FFFE, E000)- NO Item Value
    Item Tag    - 4 bytes
    Item length - 4 bytes
    BOT size    - 8 bytes

    Fragment (FFFE, E000)
    Item Tag             - 4 bytes
    Item length          - 4 bytes
    Fragment tag info    - 8 bytes

    Generator return pixel data fragment positions

    Args:
        dataset (Dataset): DICOM dataset

    Yields:
        Generator[int, int, int]:   offset - From pixel data position,
                                    current file position,
                                    fragment size in bytes

    Raises:
        InvalidDicomError: If DICOM file contains no pixel data

    """
    pixel_data = dataset.PixelData
    pixel_data_element = dataset[0x7FE0, 0x0010]
    pixel_data_offset = pixel_data_element.file_tell

    if not pixel_data_offset:
        raise pydicom3.errors.InvalidDicomError("Pixel data not found in the DICOM")

    if not pixel_data_element.is_undefined_length:
        num_of_frames = int(dataset["NumberOfFrames"].value)
        return _get_offsets_for_uncompressed_pixeldata(
            pixel_data_offset,
            pixel_data_element,
            num_of_frames,
        )
    else:
        return _get_offsets_for_encapsulated_pixeldata(
            pixel_data_offset,
            pixel_data,
        )


def get_multiframe_offset_tables(dataset: pydicom3.Dataset) -> dict:
    """
    Get offset tables for multiframe datasets.

    If the dataset is not a multiframe (i.e., it has only one frame), this function returns an empty dictionary.

    If the dataset already has both `CustomOffsetTable` and `CustomOffsetTableLengths` attributes set,
    this function returns the existing values without modification.

    Args:
        dataset (Dataset): The dataset to get offset tables from.

    Returns:
        dict: A dictionary containing the `CustomOffsetTable` and `CustomOffsetTableLengths` values.

    Notes:
        This function does not modify the dataset.
    """
    result: dict = {}
    # catch edge cases: single frame, pixeldata is None
    if (
        "NumberOfFrames" not in dataset
        or "PixelData" not in dataset
        or not dataset.NumberOfFrames
        or dataset.NumberOfFrames <= 1
        or dataset.PixelData is None
    ):
        return result

    offset_table = []
    offset_table_lengths = []

    try:
        for _, position, length in _generate_pixel_data_fragment_offsets(dataset):
            offset_table.append(position)
            offset_table_lengths.append(length)

        if dataset.NumberOfFrames == len(offset_table):
            result["CustomOffsetTable"] = offset_table
            result["CustomOffsetTableLengths"] = offset_table_lengths

    except (EOFError, ValueError):
        logger.warning("Some errors occured when creating Offset table")
    except pydicom3.errors.InvalidDicomError as error:
        logger.warning(str(error))

    return result
