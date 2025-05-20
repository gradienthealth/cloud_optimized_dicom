import random
import unittest
from io import BytesIO
from unittest.mock import patch

import numpy as np
import pydicom3
import pydicom3.encaps
import pydicom3.errors

from cloud_optimized_dicom.custom_offset_tables import get_multiframe_offset_tables


def _generate_random_pixel_data(length: int) -> bytes:
    random_array = np.random.randint(0, 256, size=length, dtype=np.uint8)
    return random_array.tobytes()


def _generate_uid() -> str:
    prefix = "1.2.826.0.1.3680043.8.498."
    total_length = 64
    last_section_length = total_length - len(prefix)
    last_section = "".join(random.choices("0123456789", k=last_section_length))
    uid = prefix + last_section
    return uid


def create_sample_dataset(
    number_of_frames=1,
    is_pixeldata_encapsulated=True,
    include_eot=False,
    include_bot=False,
) -> pydicom3.Dataset:
    """
    Creates a sample DICOM dataset.

    Parameters:
        number_of_frames (int, optional): The number of frames in the dataset. Defaults to 1.
        is_pixeldata_encapsulated (bool, optional): Whether the pixel data is encapsulated. Defaults to True.
        include_eot (bool, optional): Whether to include the Extended Offset Table (EOT) in the dataset. Defaults to False.
        include_bot (bool, optional): Whether to include the Basic Offset Table (BOT) in the dataset. Defaults to False.

    Returns:
        pydicom.Dataset: A sample DICOM dataset created using the given parameters.
    """
    file_meta = pydicom3.FileMetaDataset()
    file_meta.MediaStorageSOPClassUID = _generate_uid()
    file_meta.MediaStorageSOPInstanceUID = _generate_uid()
    file_meta.TransferSyntaxUID = _generate_uid()

    ds = pydicom3.Dataset()
    ds.file_meta = file_meta

    ds.PatientName = "Test^Patient"
    ds.PatientID = "123456"
    ds.StudyInstanceUID = _generate_uid()
    ds.SeriesInstanceUID = _generate_uid()
    ds.SOPInstanceUID = _generate_uid()
    ds.NumberOfFrames = number_of_frames

    raw_pixeldata = [_generate_random_pixel_data(200) for _ in range(number_of_frames)]

    if is_pixeldata_encapsulated:
        pixelData = pydicom3.encaps.encapsulate(raw_pixeldata, has_bot=include_bot)
    else:
        pixelData = bytes([item for sublist in raw_pixeldata for item in sublist])

    ds.is_little_endian = True
    ds.is_implicit_VR = False
    fp = BytesIO()
    ds.save_as(fp)
    pixel_data_offset = fp.tell()

    ds.PixelData = pixelData
    ds[0x7FE0, 0x0010].file_tell = pixel_data_offset
    ds[0x7FE0, 0x0010].is_undefined_length = is_pixeldata_encapsulated

    if include_eot:
        ds.ExtendedOffsetTable = bytes(_generate_random_pixel_data(number_of_frames))
        ds.ExtendedOffsetTableLengths = bytes(
            _generate_random_pixel_data(number_of_frames)
        )

    return ds


class TestMultiframeOffsetTable(unittest.TestCase):
    def test_multiframe_with_basic_offset_table(self):
        custom_offset_table = [[512, 720, 928, 1136, 1344], [200, 200, 200, 200, 200]]
        dataset = create_sample_dataset(
            number_of_frames=5,
            is_pixeldata_encapsulated=True,
            include_eot=False,
            include_bot=True,
        )

        result = get_multiframe_offset_tables(dataset)

        assert result.get("CustomOffsetTable") == custom_offset_table[0]
        assert result.get("CustomOffsetTableLengths") == custom_offset_table[1]

    def test_multiframe_with_extended_offset_table(self):
        custom_offset_table = [[492, 700, 908, 1116, 1324], [200, 200, 200, 200, 200]]
        dataset = create_sample_dataset(
            number_of_frames=5,
            is_pixeldata_encapsulated=True,
            include_eot=True,
            include_bot=False,
        )

        result = get_multiframe_offset_tables(dataset)

        assert result.get("CustomOffsetTable") == custom_offset_table[0]
        assert result.get("CustomOffsetTableLengths") == custom_offset_table[1]

    def test_multiframe_without_offset_table(self):
        custom_offset_table = [[492, 700, 908, 1116, 1324], [200, 200, 200, 200, 200]]
        dataset = create_sample_dataset(
            number_of_frames=5,
            is_pixeldata_encapsulated=True,
            include_eot=False,
            include_bot=False,
        )

        result = get_multiframe_offset_tables(dataset)

        assert result.get("CustomOffsetTable") == custom_offset_table[0]
        assert result.get("CustomOffsetTableLengths") == custom_offset_table[1]

    def test_multiframe_with_uncompressed_pixeldata(self):
        custom_offset_table = [[484, 684, 884, 1084, 1284], [200, 200, 200, 200, 200]]
        dataset = create_sample_dataset(
            number_of_frames=5,
            is_pixeldata_encapsulated=False,
            include_eot=False,
            include_bot=False,
        )

        result = get_multiframe_offset_tables(dataset)

        assert result.get("CustomOffsetTable") == custom_offset_table[0]
        assert result.get("CustomOffsetTableLengths") == custom_offset_table[1]

    def test_singleframe_uncompressed_pixeldata(self):
        dataset = create_sample_dataset(
            number_of_frames=1,
            is_pixeldata_encapsulated=False,
            include_eot=False,
            include_bot=False,
        )

        result = get_multiframe_offset_tables(dataset)

        assert "CustomOffsetTable" not in result
        assert "CustomOffsetTableLengths" not in result

    def test_singleframe_encapsulated_pixeldata(self):
        dataset = create_sample_dataset(
            number_of_frames=1,
            is_pixeldata_encapsulated=True,
            include_eot=False,
            include_bot=False,
        )

        result = get_multiframe_offset_tables(dataset)

        assert "CustomOffsetTable" not in result
        assert "CustomOffsetTableLengths" not in result

    @patch("cloud_optimized_dicom.custom_offset_tables.logger.warning")
    @patch(
        "cloud_optimized_dicom.custom_offset_tables._generate_pixel_data_fragment_offsets"
    )
    def test_multiframe_raise_value_error(
        self,
        mock_generate_pixel_data_fragment_offsets,
        mock_logging_warning,
    ):
        dataset = create_sample_dataset(
            number_of_frames=5,
            is_pixeldata_encapsulated=False,
            include_eot=False,
            include_bot=False,
        )
        mock_generate_pixel_data_fragment_offsets.side_effect = ValueError("Test error")

        get_multiframe_offset_tables(dataset)

        mock_logging_warning.assert_called_once_with(
            "Some errors occured when creating Offset table"
        )

    @patch("cloud_optimized_dicom.custom_offset_tables.logger.warning")
    @patch(
        "cloud_optimized_dicom.custom_offset_tables._generate_pixel_data_fragment_offsets"
    )
    def test_multiframe_raise_invalid_dicom_error(
        self,
        mock_generate_pixel_data_fragment_offsets,
        mock_logging_warning,
    ):
        dataset = create_sample_dataset(
            number_of_frames=5,
            is_pixeldata_encapsulated=False,
            include_eot=False,
            include_bot=False,
        )
        mock_generate_pixel_data_fragment_offsets.side_effect = (
            pydicom3.errors.InvalidDicomError("Test error")
        )

        get_multiframe_offset_tables(dataset)

        mock_logging_warning.assert_called_once_with("Test error")

    def test_pixeldata_is_none(self):
        """Edge case where the pixeldata attribute is present, but is None"""
        dataset = create_sample_dataset(
            number_of_frames=2, is_pixeldata_encapsulated=False
        )
        dataset.PixelData = None

        result = get_multiframe_offset_tables(dataset)

        assert result.get("CustomOffsetTable") is None
        assert result.get("CustomOffsetTableLengths") is None

    def test_no_pixeldata_attribute(self):
        dataset = create_sample_dataset(
            number_of_frames=2, is_pixeldata_encapsulated=False
        )
        del dataset.PixelData

        result = get_multiframe_offset_tables(dataset)

        assert result.get("CustomOffsetTable") is None
        assert result.get("CustomOffsetTableLengths") is None


if __name__ == "__main__":
    # python3 -m unittest components.cloud_optimized_dicom.tests.test_multiframe_offset_table
    unittest.main()
