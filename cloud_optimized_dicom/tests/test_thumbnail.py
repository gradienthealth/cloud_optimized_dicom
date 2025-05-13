import os
import unittest

import cv2
from google.api_core.client_options import ClientOptions
from google.cloud import storage

from cloud_optimized_dicom.cod_object import CODObject
from cloud_optimized_dicom.instance import Instance
from cloud_optimized_dicom.thumbnail.thumbnail import generate_thumbnail
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
    expected_frame_size: tuple[int, int] = (100, 100),
):
    cap = cv2.VideoCapture(os.path.join(cod_obj.temp_dir.name, "thumbnail.mp4"))
    if not cap.isOpened():
        raise ValueError("Failed to open video stream.")

    # Get the total number of frames
    frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    # get frame size (width, height)
    frame_size = (
        int(cap.get(cv2.CAP_PROP_FRAME_WIDTH)),
        int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT)),
    )

    cap.release()
    testcls.assertEqual(frame_count, expected_frame_count)
    testcls.assertEqual(frame_size, expected_frame_size)


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

    def test_generate_multiframe_thumbnail(self):
        dicom_path = os.path.join(self.test_data_dir, "multiframe.dcm")
        cod_obj = ingest_and_generate_thumbnail(
            [dicom_path], self.datastore_path, self.client
        )
        validate_thumbnail(self, cod_obj, expected_frame_count=38)
