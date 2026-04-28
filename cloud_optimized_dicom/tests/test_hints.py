import os

import pytest

from cloud_optimized_dicom.errors import HintMismatchError
from cloud_optimized_dicom.hints import Hints
from cloud_optimized_dicom.instance import Instance


def test_empty_hints(test_data_dir: str):
    hints = Hints()
    instance = Instance(
        dicom_uri=os.path.join(test_data_dir, "monochrome2.dcm"), hints=hints
    )
    assert instance.validate()


def test_good_hints(test_data_dir: str):
    hints = Hints(
        instance_uid="1.2.276.0.50.192168001092.11156604.14547392.313",
        series_uid="1.2.276.0.50.192168001092.11156604.14547392.303",
        study_uid="1.2.276.0.50.192168001092.11156604.14547392.4",
        size=527800,
        crc32c="uEaR6w==",
    )
    instance = Instance(
        dicom_uri=os.path.join(test_data_dir, "monochrome2.dcm"), hints=hints
    )
    assert instance.validate()


def test_bad_uid(test_data_dir: str):
    hints = Hints(instance_uid="BAD_UID")
    instance = Instance(
        dicom_uri=os.path.join(test_data_dir, "monochrome2.dcm"), hints=hints
    )
    with pytest.raises(HintMismatchError):
        instance.validate()


def test_bad_size(test_data_dir: str):
    hints = Hints(size=1000)
    instance = Instance(
        dicom_uri=os.path.join(test_data_dir, "monochrome2.dcm"), hints=hints
    )
    with pytest.raises(HintMismatchError):
        instance.validate()


def test_bad_crc32c(test_data_dir: str):
    hints = Hints(crc32c="BAD_CRC32C")
    instance = Instance(
        dicom_uri=os.path.join(test_data_dir, "monochrome2.dcm"), hints=hints
    )
    with pytest.raises(HintMismatchError):
        instance.validate()
