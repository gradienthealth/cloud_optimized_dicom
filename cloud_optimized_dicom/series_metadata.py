import gzip
import json
from dataclasses import dataclass, field
from io import BytesIO
from typing import Callable, Optional

from google.cloud import storage

from cloud_optimized_dicom.instance import Instance
from cloud_optimized_dicom.thumbnail import _sort_instances


@dataclass
class SeriesMetadata:
    """The metadata of an entire series.

    Parameters:
        study_uid (str): The study UID of this series (should match `CODObject.study_uid`)
        series_uid (str): The series UID of this series (should match `CODObject.series_uid`)
        hashed_uids (bool): Flag indicating whether the series uses de-identified UIDs.
        instances (dict[str, Instance]): Mapping of instance UID (hashed if `hashed_uids=True`) to Instance object
        metadata_fields (dict): Any additional user defined data
        is_sorted (bool): Flag indicating whether the instances dict is sorted

    If loading existing metadata, `hashed_uids` is inferred by the presence of the key `deid_study_uid` as opposed to `study_uid`.
    If creating new metadata, `hashed_uids` is inferred by the presence/absence of `instance.uid_hash_func` for any instances that have been added.
    """

    study_uid: str
    series_uid: str
    hashed_uids: bool
    instances: dict[str, Instance] = field(default_factory=dict)
    metadata_fields: dict = field(default_factory=dict)
    is_sorted: bool = False

    def _add_metadata_field(
        self, field_name: str, field_value, overwrite_existing=False
    ):
        """Add a custom field to the series metadata"""
        # Raise error if field exists and we're not overwriting existing fields
        if field_name in self.metadata_fields and not overwrite_existing:
            raise ValueError(
                f"Metadata field {field_name} already exists (and overwrite_existing=False)"
            )
        self.metadata_fields[field_name] = field_value

    def _remove_metadata_field(self, field_name: str) -> bool:
        """Remove a custom field from the series metadata.

        Returns:
            bool: True if the field was present and removed, False if the field was not present.
        """
        if field_name not in self.metadata_fields:
            return False
        del self.metadata_fields[field_name]
        return True

    def _sort_instances(self):
        """Sort the instances dict, the same way instances are sorted for the thumbnail.

        If sorting is successful, set `is_sorted=True`.
        If sorting is unsuccessful, set `is_sorted=False`.
        """
        # early exit if already sorted
        if self.is_sorted:
            return
        # map instances to their uids
        instance_to_uid = {instance: uid for uid, instance in self.instances.items()}
        # get a list of all instances (unsorted)
        unsorted_instances = list(instance_to_uid.keys())
        # attempt sorting
        try:
            sorted_instances = _sort_instances(unsorted_instances, strict=True)
            self.instances = {
                instance_to_uid[instance]: instance for instance in sorted_instances
            }
            self.is_sorted = True
        except ValueError:
            self.is_sorted = False

    def to_dict(self) -> dict:
        # TODO version handling once we have a new version
        study_uid_key = "deid_study_uid" if self.hashed_uids else "study_uid"
        series_uid_key = "deid_series_uid" if self.hashed_uids else "series_uid"
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
        return {**base_dict, **self.metadata_fields}

    def to_bytes(self) -> bytes:
        """Convert from SeriesMetadata -> dict -> JSON -> bytes"""
        return json.dumps(self.to_dict()).encode("utf-8")

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
    def from_dict(
        cls, series_metadata_dict: dict, uid_hash_func: Optional[Callable] = None
    ) -> "SeriesMetadata":
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
            instance_uid: Instance.from_cod_dict_v1(
                instance_dict, uid_hash_func=uid_hash_func
            )
            for instance_uid, instance_dict in cod_dict.get("instances", {}).items()
        }

        # Treat any remaining keys as metadata fields
        metadata_fields = series_metadata_dict

        return cls(
            study_uid=study_uid,
            series_uid=series_uid,
            hashed_uids=is_hashed,
            instances=instances,
            metadata_fields=metadata_fields,
        )

    @classmethod
    def from_bytes(
        cls, bytes: bytes, uid_hash_func: Optional[Callable] = None
    ) -> "SeriesMetadata":
        """Class method to create a SeriesMetadata object from a bytes object."""
        return cls.from_dict(json.loads(bytes), uid_hash_func=uid_hash_func)

    @classmethod
    def from_blob(
        cls, blob: storage.Blob, uid_hash_func: Optional[Callable] = None
    ) -> "SeriesMetadata":
        """Class method to create a SeriesMetadata object from a GCS blob."""
        return cls.from_bytes(blob.download_as_bytes(), uid_hash_func=uid_hash_func)
