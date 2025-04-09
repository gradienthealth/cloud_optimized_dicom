import gzip
import json
import logging
from dataclasses import dataclass, field
from io import BytesIO

from google.cloud import storage

from cloud_optimized_dicom.instance import Instance

logger = logging.getLogger(__name__)


@dataclass
class SeriesMetadata:
    """The metadata of an entire series.

    Parameters:
        study_uid (str): The study UID of this series (should match `CODObject.study_uid`)
        series_uid (str): The series UID of this series (should match `CODObject.series_uid`)
        instances (dict[str, Instance]): Mapping of instance UID to Instance object
        custom_tags (dict): Any additional user defined data
        _is_hashed (bool): Private property indicating whether the series uses de-identified UIDs.
        If loading existing metadata, this is inferred by the presence of the key `deid_study_uid` as opposed to `study_uid`.
        If creating new metadata, this is inferred by the presence/absence of `instance.uid_hash_func` for any instances that have been added.
    """

    study_uid: str
    series_uid: str
    instances: dict[str, Instance] = field(default_factory=dict)
    custom_tags: dict = field(default_factory=dict)
    _is_hashed: bool = False

    def _infer_is_hashed(self):
        """It is possible to infer that a series is hashed in the following ways:
        1. Pre-existing metadata: `_is_hashed` was already set to True on load, because `deid_study_uid` was present (instead of `study_uid`)
        2. Creating new metadata: all of the instances have the same `uid_hash_func` (which is not None)
        """
        # case 1: already set
        if self._is_hashed:
            return
        # if there are no instances, we cannot infer if the series is hashed
        if len(self.instances) == 0:
            logger.warning("Series has no instances, cannot infer if it is hashed")
            return
        # case 2: new metadata
        hash_funcs = set(instance.uid_hash_func for instance in self.instances.values())
        # we should never see multiple different hash functions for a series
        if len(hash_funcs) != 1:
            raise ValueError(
                "Series has instances with multiple different uid_hash_funcs, which should be impossible"
            )
        # if the hash function is not None, then the series is hashed
        self._is_hashed = hash_funcs[0] is not None

    def to_dict(self) -> dict:
        # TODO version handling once we have a new version
        # prior to saving, make sure _is_hashed is set correctly
        self._infer_is_hashed()
        study_uid_key = "deid_study_uid" if self._is_hashed else "study_uid"
        series_uid_key = "deid_series_uid" if self._is_hashed else "series_uid"
        base_dict = {
            study_uid_key: self.study_uid,
            series_uid_key: self.series_uid,
            "cod": {
                "instances": {
                    instance_uid: instance.to_cod_dict_v1()
                    for instance_uid, instance in self.instances.items()
                },
            },
        }
        return {**base_dict, **self.custom_tags}

    def to_gzipped_json(self) -> bytes:
        """Convert from SeriesMetadata -> dict -> JSON -> bytes -> gzip"""
        # TODO if memory issues continue, can try streaming dict instead of creating it outright
        series_dict = self.to_dict()
        # stream the gzip file to lower memory usage
        gzip_buffer = BytesIO()
        with gzip.GzipFile(fileobj=gzip_buffer, mode="wb") as gz:
            # Use a JSON encoder to stream the JSON data
            for chunk in json.JSONEncoder().iterencode(series_dict):
                gz.write(chunk.encode("utf-8"))
        # once compressed, file is much smaller, so we can return the bytes directly
        return gzip_buffer.getvalue()

    @classmethod
    def from_dict(cls, series_metadata_dict: dict) -> "SeriesMetadata":
        """Class method to create an instance from a dictionary."""
        # retrieve the study and series UIDs (might be de-identified)
        if "deid_study_uid" in series_metadata_dict:
            is_hashed = True
            study_uid = series_metadata_dict.pop("deid_study_uid")
            series_uid = series_metadata_dict.pop("deid_series_uid")
        else:
            is_hashed = False
            study_uid = series_metadata_dict.pop("study_uid")
            series_uid = series_metadata_dict.pop("series_uid")

        # Parse standard cod metadata
        cod_dict: dict = series_metadata_dict.pop("cod")
        instances = {
            instance_uid: Instance.from_cod_dict_v1(instance_dict)
            for instance_uid, instance_dict in cod_dict.get("instances", {}).items()
        }

        # Treat any remaining keys as custom tags
        custom_tags = series_metadata_dict

        return cls(
            study_uid=study_uid,
            series_uid=series_uid,
            instances=instances,
            custom_tags=custom_tags,
            _is_hashed=is_hashed,
        )

    @classmethod
    def from_bytes(cls, bytes: bytes) -> "SeriesMetadata":
        """Class method to create a SeriesMetadata object from a bytes object."""
        return cls.from_dict(json.loads(bytes))

    @classmethod
    def from_blob(cls, blob: storage.Blob) -> "SeriesMetadata":
        """Class method to create a SeriesMetadata object from a GCS blob."""
        return cls.from_bytes(blob.download_as_bytes())
