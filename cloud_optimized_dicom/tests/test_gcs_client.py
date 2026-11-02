"""Paths production callers take that no `BlobStore` stands in for: passing a raw
`storage.Client`, and remote instances with no `blob_store` of their own, which
COD fetches through smart_open and deletes through `transport_params["client"]`.
They need real GCS, so they run only when `ONLINE`.
"""

import os

import pytest
from google.api_core.exceptions import NotFound
from google.cloud import storage

from cloud_optimized_dicom.blob_store import BlobStore, GcsBlobStore
from cloud_optimized_dicom.cod_object import CODObject
from cloud_optimized_dicom.dicomweb import handle_request
from cloud_optimized_dicom.instance import Instance
from cloud_optimized_dicom.query_parsing import query_result_to_codobjects
from cloud_optimized_dicom.series_metadata import SeriesMetadata
from cloud_optimized_dicom.tests.conftest import ONLINE, SeriesHandle

pytestmark = pytest.mark.skipif(not ONLINE, reason="needs SISKIN_ENV_ENABLED")


def test_cod_object_with_gcs_client_round_trips(
    gcs_client: storage.Client, datastore_path: str, local_instance_path: str
):
    probe = Instance(dicom_uri=local_instance_path)
    kwargs = dict(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=probe.study_uid(),
        series_uid=probe.series_uid(),
    )
    with CODObject(mode="w", **kwargs) as cod:
        assert isinstance(cod.blob_store, GcsBlobStore)
        cod.append([Instance(dicom_uri=local_instance_path)])
    with CODObject(mode="r", **kwargs) as cod:
        assert list(cod.get_instances()) == [probe.instance_uid()]
        cod.integrity_check()


def test_query_result_with_gcs_client(
    gcs_client: storage.Client,
    blob_store: BlobStore,
    datastore_path: str,
    local_instance_path: str,
):
    probe = Instance(dicom_uri=local_instance_path)
    probe.validate()
    source_uri = f"{datastore_path}/source/{probe.instance_uid()}.dcm"
    blob_store.upload_file(source_uri, local_instance_path)
    query_result = {
        "study_uid": probe.study_uid(),
        "series_uid": probe.series_uid(),
        "files": [
            {
                "file_uri": source_uri,
                "size": probe.size(),
                "crc32c": probe.crc32c(),
                "instance_uid": probe.instance_uid(),
            }
        ],
    }

    [(cod, instances)] = query_result_to_codobjects(
        gcs_client, query_result, datastore_path
    )
    with cod:
        result = cod.append(instances)

    assert len(result.new) == 1
    assert blob_store.exists(cod.metadata_uri)


def test_dicomweb_with_gcs_client(
    gcs_client: storage.Client, seeded_series: SeriesHandle
):
    series_uri = os.path.join(
        seeded_series.datastore_path,
        "studies",
        seeded_series.study_uid,
        "series",
        seeded_series.series_uid,
        "metadata",
    )
    assert len(handle_request(series_uri, gcs_client)) == 2


def test_append_remote_instance_without_store(
    blob_store: BlobStore, datastore_path: str, local_instance_path: str
):
    """A `gs://` instance with no `blob_store` is fetched through smart_open."""
    probe = Instance(dicom_uri=local_instance_path)
    source_uri = f"{datastore_path}/source/{probe.instance_uid()}.dcm"
    blob_store.upload_file(source_uri, local_instance_path)
    with CODObject(
        client=blob_store,
        datastore_path=datastore_path,
        study_uid=probe.study_uid(),
        series_uid=probe.series_uid(),
        mode="w",
        sync_on_exit=False,
    ) as cod:
        result = cod.append([Instance(dicom_uri=source_uri)])
    assert len(result.new) == 1


def test_append_missing_remote_instance_without_store(
    blob_store: BlobStore,
    datastore_path: str,
    test_study_uid: str,
    test_series_uid: str,
):
    with CODObject(
        client=blob_store,
        datastore_path=datastore_path,
        study_uid=test_study_uid,
        series_uid=test_series_uid,
        mode="w",
        sync_on_exit=False,
    ) as cod:
        result = cod.append(
            [Instance(dicom_uri=f"{datastore_path}/source/missing.dcm")]
        )
    assert len(result.errors) == 1
    assert isinstance(result.errors[0][1], NotFound)


def test_delete_dependency_through_transport_params_client(
    gcs_client: storage.Client,
    blob_store: BlobStore,
    datastore_path: str,
    local_instance_path: str,
):
    dependency_uri = f"{datastore_path}/source/dependency.dcm"
    blob_store.upload_file(dependency_uri, local_instance_path)
    instance = Instance(
        dicom_uri=local_instance_path,
        dependencies=[dependency_uri],
        transport_params={"client": gcs_client},
    )

    deleted = instance.delete_dependencies(validate_blob_hash=True)

    assert deleted == [dependency_uri]
    assert not blob_store.exists(dependency_uri)


def test_series_metadata_from_blob(
    gcs_client: storage.Client, seeded_series: SeriesHandle
):
    with seeded_series.open(mode="r") as cod:
        metadata_uri = cod.metadata_uri
    metadata = SeriesMetadata.from_blob(
        storage.Blob.from_string(metadata_uri, client=gcs_client)
    )
    assert len(metadata.instances) == 2
