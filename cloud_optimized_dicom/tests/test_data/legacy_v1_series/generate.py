"""Regenerates the legacy v1.0 series fixture beside this file.

The fixture is a de-identified (hashed-UID) series of three synthetic images,
written by COD as it was before metadata v2 (commit 9bc029d). Every pixel and
tag is made up here; no real study goes into it. Tar headers and the index's
record of the tar's file stats are normalized, so nothing about the machine or
person that ran it goes in either.

Run it with a checkout of that commit on `PYTHONPATH`, the commit's
`requirements.txt` installed, and GCP credentials that can write to the
scratch bucket. It writes the series under a scratch prefix, downloads the
tar, index and metadata next to this file, and deletes the prefix.
"""

import json
import os
import sqlite3
import sys
import tarfile
import tempfile
import types
import uuid

import numpy as np
import pydicom3 as pydicom
import pydicom3.uid
from google.cloud import storage

# COD imported Apache Beam only for metric counters; stub it when absent.
try:
    import apache_beam  # noqa: F401
except ImportError:

    class _Metrics:
        DelegatingCounter = object

        @staticmethod
        def counter(*_):
            return types.SimpleNamespace(inc=lambda *_: None)

    _metrics = types.ModuleType("apache_beam.metrics")
    _metrics.Metrics = _Metrics
    sys.modules["apache_beam"] = types.ModuleType("apache_beam")
    sys.modules["apache_beam.metrics"] = _metrics

_gettarinfo = tarfile.TarFile.gettarinfo


def _anonymous_tarinfo(self, *args, **kwargs):
    info = _gettarinfo(self, *args, **kwargs)
    info.uid = info.gid = 0
    info.uname = info.gname = "cod"
    info.mtime = 0
    return info


# COD adds each instance with tar.add, which takes owner and mtime from the file
tarfile.TarFile.gettarinfo = _anonymous_tarinfo

from cloud_optimized_dicom.cod_object import CODObject  # noqa: E402
from cloud_optimized_dicom.instance import Instance  # noqa: E402

GCP_PROJECT = "gradient-pacs-siskin-172863"
SCRATCH_PREFIX = "gs://siskin-172863-temp/cod_fixture_gen"
FIXTURE_DIR = os.path.dirname(os.path.abspath(__file__))
NUM_INSTANCES = 3
ROWS = COLUMNS = 32


def _uid(*parts: str) -> str:
    return pydicom3.uid.generate_uid(entropy_srcs=["cod-legacy-v1-fixture", *parts])


def hash_uid(uid: str) -> str:
    """The de-identification hash the fixture's series was written with."""
    return _uid("deid", uid)


def _synthetic_instance(index: int, study_uid: str, series_uid: str) -> pydicom.Dataset:
    file_meta = pydicom.dataset.FileMetaDataset()
    file_meta.TransferSyntaxUID = pydicom3.uid.ExplicitVRLittleEndian
    file_meta.MediaStorageSOPClassUID = pydicom3.uid.SecondaryCaptureImageStorage
    file_meta.MediaStorageSOPInstanceUID = _uid("instance", str(index))
    ds = pydicom.dataset.FileDataset(
        None, {}, file_meta=file_meta, preamble=b"\0" * 128
    )
    ds.SOPClassUID = file_meta.MediaStorageSOPClassUID
    ds.SOPInstanceUID = file_meta.MediaStorageSOPInstanceUID
    ds.StudyInstanceUID = study_uid
    ds.SeriesInstanceUID = series_uid
    ds.InstanceNumber = index + 1
    ds.Modality = "OT"
    ds.PatientName = "Synthetic^Legacy"
    ds.PatientID = "SYNTHETIC-LEGACY"
    ds.PatientBirthDate = "19000101"
    ds.PatientSex = "O"
    ds.StudyDate = "20000101"
    ds.StudyTime = "000000"
    ds.AccessionNumber = "SYNTH0001"
    ds.ReferringPhysicianName = "Synthetic^Referrer"
    ds.StudyDescription = "Synthetic legacy COD fixture"
    ds.Rows = ROWS
    ds.Columns = COLUMNS
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.BitsAllocated = 16
    ds.BitsStored = 12
    ds.HighBit = 11
    ds.PixelRepresentation = 0
    ramp = np.add.outer(np.arange(ROWS), np.arange(COLUMNS)) * 32 + index
    ds.PixelData = ramp.astype(np.uint16).tobytes()
    ds.compress(pydicom3.uid.RLELossless, generate_instance_uid=False)
    return ds


def _anonymize_index_tarstats(index_path: str):
    """Zeroes the tar's file stats the index records, except `st_size`, which is
    the only one ratarmountcore checks before trusting an index."""
    with sqlite3.connect(index_path) as db:
        (raw,) = db.execute(
            "SELECT value FROM metadata WHERE key = 'tarstats'"
        ).fetchone()
        recorded = json.loads(raw)
        stats = {key: 0 for key in recorded}
        stats["st_size"] = recorded["st_size"]
        db.execute(
            "UPDATE metadata SET value = ? WHERE key = 'tarstats'",
            (json.dumps(stats),),
        )


def main():
    client = storage.Client(project=GCP_PROJECT)
    datastore_path = f"{SCRATCH_PREFIX}/{uuid.uuid4()}"
    study_uid, series_uid = _uid("study"), _uid("series")
    try:
        with tempfile.TemporaryDirectory() as source_dir:
            # relative source paths keep this machine's temp dir out of the metadata
            os.chdir(source_dir)
            instances = []
            for index in range(NUM_INSTANCES):
                path = f"{index}.dcm"
                _synthetic_instance(index, study_uid, series_uid).save_as(path)
                instances.append(Instance(dicom_uri=path, uid_hash_func=hash_uid))
            with CODObject(
                datastore_path=datastore_path,
                client=client,
                study_uid=hash_uid(study_uid),
                series_uid=hash_uid(series_uid),
                lock=True,
                hashed_uids=True,
            ) as cod:
                cod.append(instances)
                cod.sync()
                tar_uri = cod.tar_uri
                index_uri = cod.index_uri
                metadata_uri = cod.metadata_uri
            os.chdir(FIXTURE_DIR)
        for uri, name in [(tar_uri, "series.tar"), (index_uri, "index.sqlite")]:
            storage.Blob.from_string(uri, client=client).download_to_filename(
                os.path.join(FIXTURE_DIR, name)
            )
        metadata = json.loads(
            storage.Blob.from_string(metadata_uri, client=client).download_as_bytes()
        )
        with open(os.path.join(FIXTURE_DIR, "metadata.json"), "w") as f:
            json.dump(metadata, f, indent=2)
            f.write("\n")
        _anonymize_index_tarstats(os.path.join(FIXTURE_DIR, "index.sqlite"))
    finally:
        bucket_name, prefix = datastore_path[len("gs://") :].split("/", 1)
        for blob in client.list_blobs(bucket_name, prefix=f"{prefix}/"):
            blob.delete()


if __name__ == "__main__":
    main()
