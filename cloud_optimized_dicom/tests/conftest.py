import os
from dataclasses import dataclass
from typing import Literal

import gdcm
import numpy as np
import pydicom
import pydicom.examples
import pydicom.uid
import pytest
from google.api_core.client_options import ClientOptions
from google.cloud import storage

from cloud_optimized_dicom.cod_object import CODObject
from cloud_optimized_dicom.instance import Instance
from cloud_optimized_dicom.utils import delete_uploaded_blobs

GCP_PROJECT = "gradient-pacs-siskin-172863"
DATASTORE_BASE = "gs://siskin-172863-temp/cod_tests"

TEST_INSTANCE_UID = "1.2.276.0.50.192168001092.11156604.14547392.313"
TEST_SERIES_UID = "1.2.276.0.50.192168001092.11156604.14547392.303"
TEST_STUDY_UID = "1.2.276.0.50.192168001092.11156604.14547392.4"


@pytest.fixture(scope="session")
def test_data_dir() -> str:
    return os.path.join(os.path.dirname(__file__), "test_data")


@pytest.fixture(scope="session")
def local_instance_path(test_data_dir: str) -> str:
    return os.path.join(test_data_dir, "monochrome2.dcm")


@pytest.fixture(scope="session")
def gcs_client() -> storage.Client:
    return storage.Client(
        project=GCP_PROJECT,
        client_options=ClientOptions(quota_project_id=GCP_PROJECT),
    )


@pytest.fixture(scope="session")
def worker_namespace() -> str:
    """Per-(run, worker) path segment so concurrent CI runs and parallel
    xdist workers don't collide on the same GCS prefix."""
    run_id = os.environ.get("GITHUB_RUN_ID", "local")
    worker = os.environ.get("PYTEST_XDIST_WORKER", "main")
    return f"{run_id}/{worker}"


@pytest.fixture()
def datastore_path(gcs_client: storage.Client, worker_namespace: str) -> str:
    path = f"{DATASTORE_BASE}/{worker_namespace}/dicomweb"
    delete_uploaded_blobs(gcs_client, [path])
    return path


@pytest.fixture(scope="session")
def test_instance_uid() -> str:
    return TEST_INSTANCE_UID


@pytest.fixture(scope="session")
def test_series_uid() -> str:
    return TEST_SERIES_UID


@pytest.fixture(scope="session")
def test_study_uid() -> str:
    return TEST_STUDY_UID


@dataclass
class SeriesHandle:
    """Bundles the four sticky CODObject args so tests can `.open(mode=...)`
    without re-typing client/datastore_path/study_uid/series_uid every call."""

    client: storage.Client
    datastore_path: str
    study_uid: str
    series_uid: str

    def open(self, mode: Literal["r", "w", "a", "e"], **kwargs) -> CODObject:
        return CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=self.study_uid,
            series_uid=self.series_uid,
            mode=mode,
            **kwargs,
        )


@pytest.fixture(scope="module")
def series_dir(test_data_dir: str) -> str:
    return os.path.join(test_data_dir, "series")


@pytest.fixture(scope="module")
def series_files(series_dir: str) -> list[str]:
    """First two .dcm files in the series fixture directory."""
    return sorted(
        os.path.join(series_dir, f)
        for f in os.listdir(series_dir)
        if f.endswith(".dcm")
    )[:2]


@pytest.fixture(scope="module")
def series_uids(series_files: list[str]) -> tuple[str, str]:
    """Probe (study_uid, series_uid) from the first series file."""
    probe = Instance(dicom_uri=series_files[0])
    return probe.study_uid(), probe.series_uid()


@pytest.fixture
def fresh_series(
    gcs_client: storage.Client,
    datastore_path: str,
    series_uids: tuple[str, str],
) -> SeriesHandle:
    """A SeriesHandle pointing at the (cleared) datastore — no instances yet."""
    study_uid, series_uid = series_uids
    return SeriesHandle(gcs_client, datastore_path, study_uid, series_uid)


@pytest.fixture
def seeded_series(fresh_series: SeriesHandle, series_files: list[str]) -> SeriesHandle:
    """fresh_series, plus the two-file fixture ingested via mode='w'."""
    with fresh_series.open(mode="w") as cod:
        cod.append([Instance(dicom_uri=p) for p in series_files])
    return fresh_series


@pytest.fixture(scope="session")
def ybr_full_422_path() -> str:
    """A YBR_FULL_422 JPEG Baseline (lossy) multiframe DICOM. Sourced from
    pydicom's bundled example set rather than committed to this repo: pydicom
    documents `examples.ybr_color` as part of the package itself (not the
    on-demand download set), so it ships with every pydicom install."""
    return str(pydicom.examples.get_path("ybr_color"))


@pytest.fixture(scope="session")
def jpeg_lossless_path(tmp_path_factory: pytest.TempPathFactory) -> str:
    """A 16-bit MONOCHROME2 instance encoded as JPEG Lossless SV1 (.4.70)."""
    return write_encoded_image(
        tmp_path_factory.mktemp("jpeg_lossless") / "mono16_sv1.dcm",
        pydicom.uid.JPEGLosslessSV1,
        synthetic_image(),
    )


def synthetic_image(
    photometric: str = "MONOCHROME2",
    bits_allocated: int = 16,
    bits_stored: int = 12,
    is_signed: bool = False,
    number_of_frames: int = 1,
    planar_configuration: int = 0,
    rows: int = 64,
    columns: int = 64,
) -> pydicom.Dataset:
    """Builds an uncompressed (Explicit VR Little Endian) image dataset.

    The pixels are a smooth ramp with light noise, so every lossless codec
    has redundancy to remove and a re-encode can come out smaller.
    """
    rng = np.random.default_rng(seed=7)
    samples = 3 if photometric in ("RGB", "YBR_FULL") else 1
    shape = (number_of_frames, rows, columns) + ((samples,) if samples > 1 else ())
    ramp = np.add.outer(np.arange(rows), np.arange(columns)) * (
        (1 << bits_stored) // (rows + columns)
    )
    values = np.broadcast_to(ramp[..., None] if samples > 1 else ramp, shape).copy()
    values += rng.integers(0, 4, size=shape)
    if is_signed:
        values -= 1 << (bits_stored - 1)
        dtype = np.int8 if bits_allocated == 8 else np.int16
    else:
        dtype = np.uint8 if bits_allocated == 8 else np.uint16
    pixels = values.astype(dtype)
    if planar_configuration == 1:
        pixels = np.ascontiguousarray(np.moveaxis(pixels, -1, 1))

    file_meta = pydicom.dataset.FileMetaDataset()
    file_meta.TransferSyntaxUID = pydicom.uid.ExplicitVRLittleEndian
    file_meta.MediaStorageSOPClassUID = pydicom.uid.SecondaryCaptureImageStorage
    file_meta.MediaStorageSOPInstanceUID = pydicom.uid.generate_uid()
    ds = pydicom.dataset.FileDataset(
        None, {}, file_meta=file_meta, preamble=b"\0" * 128
    )
    ds.SOPClassUID = file_meta.MediaStorageSOPClassUID
    ds.SOPInstanceUID = file_meta.MediaStorageSOPInstanceUID
    ds.StudyInstanceUID = pydicom.uid.generate_uid()
    ds.SeriesInstanceUID = pydicom.uid.generate_uid()
    ds.Modality = "OT"
    ds.PatientName = "Synthetic^Fixture"
    ds.PatientID = "synthetic"
    ds.Rows = rows
    ds.Columns = columns
    ds.NumberOfFrames = number_of_frames
    ds.SamplesPerPixel = samples
    ds.PhotometricInterpretation = photometric
    ds.BitsAllocated = bits_allocated
    ds.BitsStored = bits_stored
    ds.HighBit = bits_stored - 1
    ds.PixelRepresentation = 1 if is_signed else 0
    if samples > 1:
        ds.PlanarConfiguration = planar_configuration
    if photometric == "PALETTE COLOR":
        entries = 1 << bits_stored
        for colour in ("Red", "Green", "Blue"):
            setattr(ds, f"{colour}PaletteColorLookupTableDescriptor", [entries, 0, 16])
            setattr(
                ds,
                f"{colour}PaletteColorLookupTableData",
                np.linspace(0, 65535, entries, dtype="<u2").tobytes(),
            )
    ds.PixelData = pixels.tobytes()
    return ds


def write_encoded_image(path, syntax: pydicom.uid.UID, ds: pydicom.Dataset) -> str:
    """Writes `ds` to `path` in `syntax` and returns the path as a string.

    JPEG Lossless comes from GDCM, the only encoder available for it; RLE and
    JPEG 2000 come from pydicom. Uncompressed syntaxes are written as-is.
    """
    path = str(path)
    if syntax in (pydicom.uid.JPEGLossless, pydicom.uid.JPEGLosslessSV1):
        uncompressed = path + ".explicit.dcm"
        ds.save_as(uncompressed, enforce_file_format=True)
        _gdcm_transcode(uncompressed, path, syntax)
        os.remove(uncompressed)
        return path
    if syntax.is_compressed:
        ds.compress(syntax, generate_instance_uid=False)
    ds.save_as(path, enforce_file_format=True)
    return path


_GDCM_SYNTAXES = {
    pydicom.uid.JPEGLossless: gdcm.TransferSyntax.JPEGLosslessProcess14,
    pydicom.uid.JPEGLosslessSV1: gdcm.TransferSyntax.JPEGLosslessProcess14_1,
}


def _gdcm_transcode(input_path: str, output_path: str, syntax: pydicom.uid.UID):
    reader = gdcm.ImageReader()
    reader.SetFileName(input_path)
    assert reader.Read(), f"GDCM could not read {input_path}"
    change = gdcm.ImageChangeTransferSyntax()
    change.SetTransferSyntax(gdcm.TransferSyntax(_GDCM_SYNTAXES[syntax]))
    change.SetInput(reader.GetImage())
    assert change.Change(), f"GDCM could not encode {input_path} as {syntax.name}"
    writer = gdcm.ImageWriter()
    writer.SetFileName(output_path)
    writer.SetFile(reader.GetFile())
    writer.SetImage(change.GetOutput())
    assert writer.Write(), f"GDCM could not write {output_path}"
