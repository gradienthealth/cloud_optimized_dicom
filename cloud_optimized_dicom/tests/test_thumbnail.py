import os
import unittest

from google.api_core.client_options import ClientOptions
from google.cloud import storage

from cloud_optimized_dicom.cod_object import CODObject
from cloud_optimized_dicom.instance import Instance
from cloud_optimized_dicom.thumbnail import generate_thumbnail
from cloud_optimized_dicom.utils import delete_uploaded_blobs


class TestThumbnail(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")
        cls.test_instance_uid = "1.2.276.0.50.192168001092.11156604.14547392.313"
        cls.test_series_uid = "1.2.276.0.50.192168001092.11156604.14547392.303"
        cls.test_study_uid = "1.2.276.0.50.192168001092.11156604.14547392.4"
        cls.local_instance_path = os.path.join(cls.test_data_dir, "valid.dcm")
        cls.client = storage.Client(
            project="gradient-pacs-siskin-172863",
            client_options=ClientOptions(
                quota_project_id="gradient-pacs-siskin-172863"
            ),
        )
        cls.datastore_path = "gs://siskin-172863-temp/cod_thumbnail_tests/dicomweb"
        delete_uploaded_blobs(cls.client, [cls.datastore_path])

    def test_generate_thumbnail(self):
        instance = Instance(dicom_uri=self.local_instance_path)
        with CODObject(
            datastore_path=self.datastore_path,
            client=self.client,
            study_uid=self.test_study_uid,
            series_uid=self.test_series_uid,
            lock=False,
        ) as cod_obj:
            cod_obj.append([instance], dirty=True)
            generate_thumbnail(cod_obj, dirty=True)
