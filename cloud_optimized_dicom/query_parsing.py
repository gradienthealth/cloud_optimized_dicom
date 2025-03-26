import logging
from itertools import groupby
from typing import Iterator

from google.cloud import storage

from cloud_optimized_dicom.cod_object import CODObject
from cloud_optimized_dicom.errors import LockAcquisitionError
from cloud_optimized_dicom.instance import Hints, Instance

logger = logging.getLogger(__name__)

SERIES_RATIO_WARNING_THRESHOLD = 0.5


def query_result_to_codobjects(
    client: storage.Client,
    query_result: dict,
    datastore_path: str,
    validate_datastore_path: bool = True,
    lock: bool = True,
) -> list[tuple[CODObject, list[Instance]]]:
    """Helper that calls query_result_to_instances and instances_to_codobj_tuples in sequence"""
    instances = query_result_to_instances(query_result)
    return list(
        instances_to_codobj_tuples(
            client, instances, datastore_path, validate_datastore_path, lock
        )
    )


def query_result_to_instances(query_result: dict) -> list[Instance]:
    """Convert a bigquery results dict into a list of instances"""
    assert "files" in query_result
    assert isinstance(query_result["files"], list)
    instances: list[Instance] = []
    study_uid = query_result["study_uid"]
    series_uid = query_result["series_uid"]
    for file in query_result["files"]:
        # assert uri provided
        if not (file_uri := file.get("file_uri", None)):
            raise AttributeError(
                f"'file_uri' field missing from file within query:\n{file}"
            )
        # make the instance
        instance = Instance(
            dicom_uri=file_uri,
            _original_path=file_uri,
            hints=Hints.from_bigquery_file_dict(file),
            dependencies=[file_uri],
        )
        # if query provided UIDs, assume they're right. Set hints (will allow us to skip fetching later)
        if study_uid and series_uid:
            instance.hints.deid_study_uid = study_uid
            instance.hints.deid_series_uid = series_uid
        instances.append(instance)
    return instances


def instances_to_codobj_tuples(
    client: storage.Client,
    instances: list[Instance],
    datastore_path: str,
    validate_datastore_path: bool = True,
    lock: bool = True,
) -> Iterator[tuple[CODObject, list[Instance]]]:
    """Group instances by study/series, make codobjects, and yield (codobj, instances) pairs"""
    # need to set client on instances before sorting (may have to fetch them)
    for instance in instances:
        instance.client = client

    # sort instances prior to grouping (groupby requires a sorted list)
    instances.sort(key=lambda x: (x._hints_deid_study_uid, x._hints_deid_series_uid))

    num_series = 0
    for deid_study_series_tuple, series_instances in groupby(
        instances, lambda x: (x._hints_deid_study_uid, x._hints_deid_series_uid)
    ):
        # form instances into list
        instances_list = list(series_instances)
        deid_study_uid, deid_series_uid = deid_study_series_tuple
        try:
            cod_obj = CODObject(
                datastore_path=datastore_path,
                client=client,
                study_uid=deid_study_uid,
                series_uid=deid_series_uid,
                lock=lock,
                _validate_datastore_path=validate_datastore_path,
            )
            num_series += 1
            yield (cod_obj, instances_list)
        except LockAcquisitionError as e:
            logger.warning(
                f"GRADIENT_STATE_LOGS:LOCK:ACQUISITION_FAILED:STUDY:{deid_study_uid}:SERIES:{deid_series_uid}:{e}"
            )
        except Exception as e:
            logger.exception(
                f"GRADIENT_STATE_LOGS:CODOBJ_INIT_FAILED:STUDY:{deid_study_uid}:SERIES:{deid_series_uid}:ERROR:{e}"
            )

    # Log warning about series ratio after all processing
    num_instances = len(instances)
    if (
        num_instances > 1
        and num_series / num_instances > SERIES_RATIO_WARNING_THRESHOLD
    ):
        logger.warning(
            f"POOR GROUPING DETECTED: created {num_series} series for {num_instances} instances. Consider different grouping logic"
        )
