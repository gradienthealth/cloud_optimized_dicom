import os
from dataclasses import dataclass
from typing import Literal

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
