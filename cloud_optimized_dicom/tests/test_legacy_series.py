"""Reads a de-identified series that COD wrote before metadata v2.

The fixture in `test_data/legacy_v1_series` (built by its `generate.py` from
synthetic images) has v1.0 instance metadata, `deid_*` series keys, and an
index from an older ratarmountcore. Datastores still hold series like it, so
the current reader has to serve them.
"""

import gzip
import json
import os
from dataclasses import dataclass

import numpy as np
import pydicom
import pytest

from cloud_optimized_dicom.blob_store import BlobStore
from cloud_optimized_dicom.cod_object import CODObject
from cloud_optimized_dicom.dicomweb import STUDY_LEVEL_TAGS, handle_request

LEGACY_DIR = os.path.join(os.path.dirname(__file__), "test_data", "legacy_v1_series")
NUM_INSTANCES = 3
# generate.py's synthetic images are 32x32, 16-bit, RLE Lossless
ROWS = COLUMNS = 32
RLE_SEGMENTS_PER_16_BIT_SAMPLE = 2


@dataclass(frozen=True)
class LegacySeries:
    client: BlobStore
    datastore_path: str
    study_uid: str
    series_uid: str

    @property
    def series_uri(self) -> str:
        return os.path.join(
            self.datastore_path, "studies", self.study_uid, "series", self.series_uid
        )

    def open(self) -> CODObject:
        return CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=self.study_uid,
            series_uid=self.series_uid,
            mode="r",
            hashed_uids=True,
        )


@pytest.fixture
def legacy_series(blob_store: BlobStore, datastore_path: str) -> LegacySeries:
    """The fixture's tar, index and metadata, uploaded as the old writer laid them out."""
    with open(os.path.join(LEGACY_DIR, "metadata.json"), "rb") as f:
        metadata_bytes = f.read()
    metadata = json.loads(metadata_bytes)
    series = LegacySeries(
        client=blob_store,
        datastore_path=datastore_path,
        study_uid=metadata["deid_study_uid"],
        series_uid=metadata["deid_series_uid"],
    )
    blob_store.upload_file(
        f"{series.series_uri}.tar", os.path.join(LEGACY_DIR, "series.tar")
    )
    blob_store.upload_file(
        os.path.join(series.series_uri, "index.sqlite"),
        os.path.join(LEGACY_DIR, "index.sqlite"),
    )
    blob_store.upload_bytes(
        os.path.join(series.series_uri, "metadata.json"),
        gzip.compress(metadata_bytes),
        content_type="application/json",
        content_encoding="gzip",
    )
    return series


def test_legacy_metadata_loads(legacy_series: LegacySeries):
    with legacy_series.open() as cod:
        instances = cod.get_instances()
    assert len(instances) == NUM_INSTANCES
    assert all(instance.has_pixeldata for instance in instances.values())


def test_legacy_tar_matches_metadata_through_old_index(legacy_series: LegacySeries):
    """integrity_check opens the tar through the old index and compares every
    instance's crc32c with the v1.0 metadata."""
    with legacy_series.open() as cod:
        cod.integrity_check()


def test_open_legacy_instance(legacy_series: LegacySeries):
    with legacy_series.open() as cod:
        instance_uid, instance = next(iter(cod.get_instances().items()))
        with cod.open_instance(instance_uid) as f:
            ds = pydicom.dcmread(f)
    expected_index = ds.InstanceNumber - 1
    expected = np.add.outer(np.arange(ROWS), np.arange(COLUMNS)) * 32 + expected_index
    assert ds.SOPInstanceUID == instance.metadata["00080018"]["Value"][0]
    np.testing.assert_array_equal(ds.pixel_array, expected)


def test_dicomweb_study_on_legacy_series(legacy_series: LegacySeries):
    study_uri = os.path.join(
        legacy_series.datastore_path, "studies", legacy_series.study_uid, "metadata"
    )
    result = handle_request(study_uri, legacy_series.client)
    for tag in STUDY_LEVEL_TAGS:
        assert result[tag]["Value"][0] is not None


def test_dicomweb_series_on_legacy_series(legacy_series: LegacySeries):
    result = handle_request(
        os.path.join(legacy_series.series_uri, "metadata"), legacy_series.client
    )
    assert len(result) == NUM_INSTANCES


def test_dicomweb_frame_on_legacy_series(legacy_series: LegacySeries):
    """The ranged read must land exactly on the instance, or the frame will not
    start with an RLE header."""
    with legacy_series.open() as cod:
        instance_uid = next(iter(cod.get_instances(strict_sorting=False)))
    frame_uri = os.path.join(
        legacy_series.series_uri, "instances", instance_uid, "frames", "1"
    )
    result = handle_request(frame_uri, legacy_series.client)
    assert len(result) == 1
    assert int.from_bytes(result[0][:4], "little") == RLE_SEGMENTS_PER_16_BIT_SAMPLE
