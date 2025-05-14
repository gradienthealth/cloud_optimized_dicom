import os
import unittest

import cv2
import numpy as np
import pydicom3
import pydicom3.encaps
from google.api_core.client_options import ClientOptions
from google.cloud import storage

from cloud_optimized_dicom.cod_object import CODObject
from cloud_optimized_dicom.instance import Instance
from cloud_optimized_dicom.thumbnail.thumbnail import generate_thumbnail
from cloud_optimized_dicom.thumbnail.utils import DEFAULT_SIZE
from cloud_optimized_dicom.utils import delete_uploaded_blobs


def ingest_and_generate_thumbnail(
    instance_paths: list[str], datastore_path: str, client: storage.Client
):
    instances = [Instance(dicom_uri=path) for path in instance_paths]
    with CODObject(
        datastore_path=datastore_path,
        client=client,
        study_uid=instances[0].study_uid(),
        series_uid=instances[0].series_uid(),
        lock=False,
    ) as cod_obj:
        cod_obj.append(instances, dirty=True)
        generate_thumbnail(cod_obj, dirty=True)
        return cod_obj


def validate_thumbnail(
    testcls: unittest.TestCase,
    cod_obj: CODObject,
    expected_frame_count: int,
    expected_frame_size: tuple[int, int] = (DEFAULT_SIZE, DEFAULT_SIZE),
    save_loc: str = None,
):
    thumbnail_name = "thumbnail.mp4" if expected_frame_count > 1 else "thumbnail.jpg"
    thumbnail_path = os.path.join(cod_obj.temp_dir.name, thumbnail_name)
    cap = cv2.VideoCapture(thumbnail_path)
    if not cap.isOpened():
        raise ValueError("Failed to open video stream.")

    # Get the total number of frames
    frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    # get frame size (width, height)
    frame_size = (
        int(cap.get(cv2.CAP_PROP_FRAME_WIDTH)),
        int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT)),
    )

    # Check content variation for each frame
    for _ in range(frame_count):
        ret, frame = cap.read()
        if not ret:
            break
        # Calculate standard deviation of pixel values
        std_dev = frame.std()
        # Assert that there is meaningful variation (not a blank/black image)
        testcls.assertGreater(std_dev, 10.0, "Thumbnail appears to be blank or uniform")

    cap.release()
    testcls.assertEqual(frame_count, expected_frame_count)
    testcls.assertEqual(frame_size, expected_frame_size)
    if save_loc:
        with open(save_loc, "wb") as f, open(thumbnail_path, "rb") as f2:
            f.write(f2.read())


class TestThumbnail(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")
        cls.project = "gradient-pacs-siskin-172863"
        cls.client = storage.Client(
            project=cls.project,
            client_options=ClientOptions(quota_project_id=cls.project),
        )
        cls.datastore_path = "gs://siskin-172863-temp/cod_thumbnail_tests/dicomweb"

    def setUp(self):
        delete_uploaded_blobs(self.client, [self.datastore_path])

    def test_gen_jpg(self):
        dicom_path = os.path.join(self.test_data_dir, "valid.dcm")
        cod_obj = ingest_and_generate_thumbnail(
            [dicom_path], self.datastore_path, self.client
        )
        validate_thumbnail(
            self, cod_obj, expected_frame_count=1, save_loc="./thumbnail.jpg"
        )

    def test_gen_mp4(self):
        series_folder = os.path.join(self.test_data_dir, "series")
        dicom_paths = [
            os.path.join(series_folder, f)
            for f in os.listdir(series_folder)
            if f.endswith(".dcm")
        ]
        cod_obj = ingest_and_generate_thumbnail(
            dicom_paths, self.datastore_path, self.client
        )
        validate_thumbnail(
            self, cod_obj, expected_frame_count=10, save_loc="./thumbnail.mp4"
        )

    def test_gen_multiframe(self):
        multiframe_path = os.path.join(self.test_data_dir, "multiframe.dcm")
        cod_obj = ingest_and_generate_thumbnail(
            [multiframe_path], self.datastore_path, self.client
        )
        validate_thumbnail(
            self, cod_obj, expected_frame_count=78, save_loc="./thumbnail.mp4"
        )
