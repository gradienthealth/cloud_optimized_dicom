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
) -> tuple[CODObject, np.ndarray]:
    instances = [Instance(dicom_uri=path) for path in instance_paths]
    with CODObject(
        datastore_path=datastore_path,
        client=client,
        study_uid=instances[0].study_uid(),
        series_uid=instances[0].series_uid(),
        lock=False,
    ) as cod_obj:
        cod_obj.append(instances, dirty=True)
        return cod_obj, cod_obj.get_thumbnail(dirty=True)


def validate_thumbnail(
    testcls: unittest.TestCase,
    thumbnail: np.ndarray,
    cod_obj: CODObject,
    expected_frame_count: int,
    expected_frame_size: tuple[int, int] = (DEFAULT_SIZE, DEFAULT_SIZE),
    dirty: bool = True,
):
    testcls.assertTrue(
        len(thumbnail.shape) == 3 or len(thumbnail.shape) == 4,
        "Thumbnail must be a 3D or 4D array",
    )
    # 3D array -> jpg -> (H, W, 3); 4D array -> mp4 -> (N, H, W, 3)
    num_frames = thumbnail.shape[0] if len(thumbnail.shape) > 3 else 1
    frame_size = (
        thumbnail.shape[1:3] if len(thumbnail.shape) > 3 else thumbnail.shape[0:2]
    )
    testcls.assertEqual(
        num_frames,
        expected_frame_count,
        f"Expected {expected_frame_count} frames, got {num_frames}",
    )
    testcls.assertEqual(
        frame_size,
        expected_frame_size,
        f"Expected frame size {expected_frame_size}, got {frame_size}",
    )

    # test the thumbnail coord converter
    instance_uid = list(
        cod_obj.get_custom_tag("thumbnail", dirty=dirty)["instances"].keys()
    )[0]
    thumbnail_frame_metadata = cod_obj.get_custom_tag("thumbnail", dirty=dirty)[
        "instances"
    ][instance_uid]["frames"][0]
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
        cod_obj, thumbnail = ingest_and_generate_thumbnail(
            [dicom_path], self.datastore_path, self.client
        )
        validate_thumbnail(self, thumbnail, cod_obj, expected_frame_count=1)
        reloaded_metadata = SeriesMetadata.from_bytes(cod_obj._metadata.to_bytes())
        self.assertIsNotNone(reloaded_metadata.custom_tags["thumbnail"])
        self.assertDictEqual(
            reloaded_metadata.custom_tags["thumbnail"],
            cod_obj._metadata.custom_tags["thumbnail"],
        )

    def test_gen_monochrome2(self):
        """Test thumbnail generation for a single frame DICOM file (MONOCHROME2)"""
        dicom_path = os.path.join(self.test_data_dir, "monochrome2.dcm")
        cod_obj, thumbnail = ingest_and_generate_thumbnail(
            [dicom_path], self.datastore_path, self.client
        )
        validate_thumbnail(self, thumbnail, cod_obj, expected_frame_count=1)

    def test_gen_mp4_mixed_phot_interp(self):
        """Test thumbnail generation for a series of DICOM files with different photometric interpretations (YBR_RCT and MONOCHROME2)"""
        series_folder = os.path.join(self.test_data_dir, "series")
        dicom_paths = [
            os.path.join(series_folder, f)
            for f in os.listdir(series_folder)
            if f.endswith(".dcm")
        ]
        cod_obj, thumbnail = ingest_and_generate_thumbnail(
            dicom_paths, self.datastore_path, self.client
        )
        validate_thumbnail(self, thumbnail, cod_obj, expected_frame_count=10)

    def test_gen_mp4_ybr_rct_multiframe(self):
        """Test thumbnail generation for a multiframe DICOM file (YBR_RCT)"""
        multiframe_path = os.path.join(self.test_data_dir, "ybr_rct_multiframe.dcm")
        cod_obj, thumbnail = ingest_and_generate_thumbnail(
            [multiframe_path], self.datastore_path, self.client
        )
        validate_thumbnail(self, thumbnail, cod_obj, expected_frame_count=78)
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
            cod_obj.get_thumbnail()
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
            thumbnail = cod_obj.get_thumbnail(dirty=True)
            # thumbnail is now synced
            self.assertTrue(cod_obj._thumbnail_synced)
            validate_thumbnail(self, thumbnail, cod_obj, expected_frame_count=1)

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
            thumbnail = cod_obj.get_thumbnail()
            validate_thumbnail(
                self, thumbnail, cod_obj, expected_frame_count=1, dirty=False
            )
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
            thumbnail = cod_obj.get_thumbnail()
            validate_thumbnail(
                self, thumbnail, cod_obj, expected_frame_count=2, dirty=False
            )
