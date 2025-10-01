from itertools import groupby
from typing import Callable, Iterator

from google.api_core.exceptions import NotFound
from google.cloud import storage

from cloud_optimized_dicom.cod_object import CODObject
from cloud_optimized_dicom.config import logger
from cloud_optimized_dicom.errors import LockAcquisitionError
from cloud_optimized_dicom.instance import Hints, Instance
from cloud_optimized_dicom.metrics import INSTANCES_NOT_FOUND

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
    logger.info(f"Found {len(instances)} instances")
    return list(
        instances_to_codobj_tuples(
            client, instances, datastore_path, validate_datastore_path, lock
        )
    )


def query_result_to_instances(
    query_result: dict, uid_hash_func: Callable[[str], str] = None
) -> list[Instance]:
    """Convert a bigquery results dict into a list of instances"""
    assert "files" in query_result
    assert isinstance(query_result["files"], list)
    instances: list[Instance] = []
    study_uid = query_result.get("study_uid")
    series_uid = query_result.get("series_uid")
    for file in query_result["files"]:
        # assert uri provided
        if not (file_uri := file.get("file_uri", None)):
            raise AttributeError(
                f"'file_uri' field missing from file within query:\n{file}"
            )
        hints = Hints(
            size=file.get("size"),
            crc32c=file.get("crc32c"),
            instance_uid=file.get("instance_uid"),
            study_uid=study_uid,
            series_uid=series_uid,
        )
        # make the instance
        instance = Instance(
            dicom_uri=file_uri,
            dependencies=[file_uri],
            hints=hints,
            uid_hash_func=uid_hash_func,
            _original_path=file_uri,
        )
        instances.append(instance)
    return instances


def get_uids_for_cod_obj(
    uid_tuple: tuple[str, str], instances: list[Instance]
) -> tuple[str, str]:
    """Given the study/series UIDs from the groupby() call, which are true UIDs,
    Determine whether hashed uids are available/should be used (all instances have a uid_hash_func provided).
    Return hashed study/series UIDs if so, otherwise return standard UIDs. Also return a bool representing whether they're hashed
    """
    instance_uid_hash_func = instances[0].uid_hash_func
    # sanity check: all instances must have same hash func (it could be None which is ok)
    assert all(
        i.uid_hash_func == instance_uid_hash_func for i in instances
    ), "not all instances have the same uid hash function"
    # if hash func is provided, return hashed study/series uids for use in cod obj path
    study_uid, series_uid = uid_tuple
    if instance_uid_hash_func is not None:
        return (
            instance_uid_hash_func(study_uid),
            instance_uid_hash_func(series_uid),
            True,
        )
    # if we get here, hash func is None. Just return standard UIDs
    return study_uid, series_uid, False


def fetch_instances_without_hints(raw_instances: list[Instance]) -> list[Instance]:
    """We need study/series uids to group instances. Hints can be used if provided, but if they're not we need to fetch the instances.
    Returns a list of instances that either have been fetched successfully or have study/series uid hints.
    """
    instances = []
    for instance in raw_instances:
        # if study/series uid hints are provided, use them
        if instance.hints.study_uid and instance.hints.series_uid:
            instances.append(instance)
            continue
        # if study/series uid hints are not provided, attempt to fetch the instance
        try:
            instance.fetch()
            instances.append(instance)
        except NotFound:
            INSTANCES_NOT_FOUND.inc()
            logger.warning(
                f"Skipping instance (tried to fetch but not found): {instance}"
            )
        except Exception as e:
            logger.exception(
                f"Skipping instance (error while fetching): {instance}: {e}"
            )
    return instances


def instances_to_codobj_tuples(
    client: storage.Client,
    instances: list[Instance],
    datastore_path: str,
    empty_lock_override_age: float = None,
    lock: bool = True,
) -> Iterator[tuple[CODObject, list[Instance]]]:
    """Group instances by study/series, make codobjects, and yield (codobj, instances) pairs"""
    # need to set client on instances before sorting (may have to fetch them)
    for instance in instances:
        instance.transport_params = dict(client=client)

    # Need study/series uids to sort/group instances. Can use hints if provided, otherwise must fetch
    instances = fetch_instances_without_hints(instances)

    # sort instances prior to grouping (groupby requires a sorted list)
    instances.sort(
        key=lambda x: (
            x.study_uid(trust_hints_if_available=True),
            x.series_uid(trust_hints_if_available=True),
        )
    )

    num_series = 0
    for study_series_uid_tuple, series_instances in groupby(
        instances,
        lambda x: (
            x.study_uid(trust_hints_if_available=True),
            x.series_uid(trust_hints_if_available=True),
        ),
    ):
        # form instances into list
        instances_list = list(series_instances)
        study_uid, series_uid, hashed_uids = get_uids_for_cod_obj(
            study_series_uid_tuple, instances_list
        )
        try:
            cod_obj = CODObject(
                datastore_path=datastore_path,
                client=client,
                study_uid=study_uid,
                series_uid=series_uid,
                lock=lock,
                hashed_uids=hashed_uids,
                empty_lock_override_age=empty_lock_override_age,
            )
            num_series += 1
            yield (cod_obj, instances_list)
        except LockAcquisitionError as e:
            logger.exception(
                f"COD:LOCK:ACQUISITION_FAILED:STUDY:{study_uid}:SERIES:{series_uid}:{e}"
            )
        except Exception as e:
            logger.exception(
                f"COD:CODOBJ_INIT_FAILED:STUDY:{study_uid}:SERIES:{series_uid}:ERROR:{e}"
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
