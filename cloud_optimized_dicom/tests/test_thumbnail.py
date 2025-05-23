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
from cloud_optimized_dicom.series_metadata import SeriesMetadata
from cloud_optimized_dicom.tests.test_hashed_uids import example_hash_function
from cloud_optimized_dicom.thumbnail import DEFAULT_SIZE, ThumbnailCoordConverter
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
        cod_obj.generate_thumbnail(dirty=True)
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

    # test the thumbnail coord converter
    instance_uid = list(cod_obj._metadata.custom_tags["thumbnail"]["instances"].keys())[
        0
    ]
    thumbnail_frame_metadata = cod_obj._metadata.custom_tags["thumbnail"]["instances"][
        instance_uid
    ]["frames"][0]
    converter = ThumbnailCoordConverter.from_anchors(
        thumbnail_frame_metadata["anchors"]
    )
    # bigger dimension should be expected_frame_size
    testcls.assertEqual(max(converter.thmb_w, converter.thmb_h), DEFAULT_SIZE)
    # aspect ratio should be the same as the original image
    testcls.assertAlmostEqual(
        converter.thmb_w / converter.thmb_h,
        converter.orig_w / converter.orig_h,
        places=2,
    )
    # convert point on original image to thumbnail and back
    test_point = (10, 10)
    recovered_point = converter.thumbnail_to_original(
        converter.original_to_thumbnail(test_point)
    )
    testcls.assertAlmostEqual(recovered_point[0], test_point[0])
    testcls.assertAlmostEqual(recovered_point[1], test_point[1])


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

    def test_gen_monochrome1(self):
        """Test thumbnail generation for a single frame DICOM file (MONOCHROME1)"""
        dicom_path = os.path.join(self.test_data_dir, "monochrome1.dcm")
        cod_obj = ingest_and_generate_thumbnail(
            [dicom_path], self.datastore_path, self.client
        )
        validate_thumbnail(self, cod_obj, expected_frame_count=1)
        reloaded_metadata = SeriesMetadata.from_bytes(cod_obj._metadata.to_bytes())
        self.assertIsNotNone(reloaded_metadata.custom_tags["thumbnail"])
        self.assertDictEqual(
            reloaded_metadata.custom_tags["thumbnail"],
            cod_obj._metadata.custom_tags["thumbnail"],
        )

    def test_gen_monochrome2(self):
        """Test thumbnail generation for a single frame DICOM file (MONOCHROME2)"""
        dicom_path = os.path.join(self.test_data_dir, "monochrome2.dcm")
        cod_obj = ingest_and_generate_thumbnail(
            [dicom_path], self.datastore_path, self.client
        )
        validate_thumbnail(self, cod_obj, expected_frame_count=1)

    def test_gen_mp4_mixed_phot_interp(self):
        """Test thumbnail generation for a series of DICOM files with different photometric interpretations (YBR_RCT and MONOCHROME2)"""
        series_folder = os.path.join(self.test_data_dir, "series")
        dicom_paths = [
            os.path.join(series_folder, f)
            for f in os.listdir(series_folder)
            if f.endswith(".dcm")
        ]
        cod_obj = ingest_and_generate_thumbnail(
            dicom_paths, self.datastore_path, self.client
        )
        validate_thumbnail(self, cod_obj, expected_frame_count=10)

    def test_gen_mp4_ybr_rct_multiframe(self):
        """Test thumbnail generation for a multiframe DICOM file (YBR_RCT)"""
        multiframe_path = os.path.join(self.test_data_dir, "ybr_rct_multiframe.dcm")
        cod_obj = ingest_and_generate_thumbnail(
            [multiframe_path], self.datastore_path, self.client
        )
        validate_thumbnail(self, cod_obj, expected_frame_count=78)
        reloaded_metadata = SeriesMetadata.from_bytes(cod_obj._metadata.to_bytes())
        self.assertIsNotNone(reloaded_metadata.custom_tags["thumbnail"])
        self.assertDictEqual(
            reloaded_metadata.custom_tags["thumbnail"],
            cod_obj._metadata.custom_tags["thumbnail"],
        )

    def test_sync_and_fetch(self):
        """Test thumbnail generation and sync"""
        # create and sync thumbnail
        instance = Instance(
            dicom_uri=os.path.join(self.test_data_dir, "monochrome1.dcm")
        )
        with CODObject(
            datastore_path=self.datastore_path,
            client=self.client,
            study_uid=instance.study_uid(),
            series_uid=instance.series_uid(),
            lock=True,
        ) as cod_obj:
            cod_obj.append([instance])
            cod_obj.generate_thumbnail()
            cod_obj.sync()
        # with a new cod object, fetch and validate thumbnail
        with CODObject(
            datastore_path=self.datastore_path,
            client=self.client,
            study_uid=instance.study_uid(),
            series_uid=instance.series_uid(),
            lock=False,
        ) as cod_obj:
            # thumbnail is not synced - it exists in datastore, but we haven't pulled it
            self.assertFalse(cod_obj._thumbnail_synced)
            thumbnail_path = cod_obj.fetch_thumbnail(dirty=True)
            # thumbnail is now synced
            self.assertTrue(cod_obj._thumbnail_synced)
            self.assertTrue(os.path.exists(thumbnail_path))
            validate_thumbnail(self, cod_obj, expected_frame_count=1)

    def test_update_existing_thumbnail(self):
        """Test that updating an existing thumbnail works"""
        instance_a = Instance(
            dicom_uri=os.path.join(
                self.test_data_dir,
                "series",
                "1.2.826.0.1.3680043.8.498.22997958494980951977704130269567444795.dcm",
            ),
            uid_hash_func=example_hash_function,
        )
        with CODObject(
            datastore_path=self.datastore_path,
            client=self.client,
            study_uid=instance_a.hashed_study_uid(),
            series_uid=instance_a.hashed_series_uid(),
            hashed_uids=True,
            lock=True,
        ) as cod_obj:
            cod_obj.append([instance_a])
            thumbnail_path = cod_obj.generate_thumbnail()
            self.assertTrue(os.path.exists(thumbnail_path))
            self.assertTrue(thumbnail_path.endswith(".jpg"))
            cod_obj.sync()
        instance_b = Instance(
            dicom_uri=os.path.join(
                self.test_data_dir,
                "series",
                "1.2.826.0.1.3680043.8.498.28109707839310833322020505651875585013.dcm",
            ),
            uid_hash_func=example_hash_function,
        )
        with CODObject(
            datastore_path=self.datastore_path,
            client=self.client,
            study_uid=instance_b.hashed_study_uid(),
            series_uid=instance_b.hashed_series_uid(),
            hashed_uids=True,
            lock=True,
        ) as cod_obj:
            cod_obj.append([instance_b])
            thumbnail_path = cod_obj.generate_thumbnail(overwrite_existing=True)
            self.assertTrue(os.path.exists(thumbnail_path))
            self.assertTrue(thumbnail_path.endswith(".mp4"))
