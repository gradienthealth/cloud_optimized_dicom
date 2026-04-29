import os

import pytest
from google.api_core.client_options import ClientOptions
from google.cloud import storage

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
