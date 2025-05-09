import unittest

from google.api_core.client_options import ClientOptions
from google.cloud import storage

from cloud_optimized_dicom.cod_object import CODObject
from cloud_optimized_dicom.instance import Instance
from cloud_optimized_dicom.thumbnail.thumbnail import generate_thumbnail
from cloud_optimized_dicom.utils import delete_uploaded_blobs


class TestThumbnail(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.series_uri = "gs://auritus-681591-pacs-deid/v1.0/dicomweb/studies/1.2.826.0.1.3680043.8.498.10001512690545661607117237232241841743/series/1.2.826.0.1.3680043.8.498.51513628843911584889313064629860199507"
        cls.client = storage.Client(
            project="gradient-pacs-auritus-681591",
            client_options=ClientOptions(
                quota_project_id="gradient-pacs-auritus-681591"
            ),
        )
        cls.datastore_path = "gs://siskin-172863-temp/cod_thumbnail_tests/dicomweb"
        delete_uploaded_blobs(cls.client, [cls.datastore_path])

    def test_generate_thumbnail(self):
        with CODObject.from_uri(
            uri=self.series_uri,
            client=self.client,
            lock=False,
            hashed_uids=True,
            create_if_missing=False,
        ) as cod_obj:
            generate_thumbnail(cod_obj, dirty=True)
