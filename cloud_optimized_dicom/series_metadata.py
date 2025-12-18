import gzip
import json
from dataclasses import dataclass, field
from io import BytesIO
from typing import Callable, Optional

from google.cloud import storage

from cloud_optimized_dicom.config import logger
from cloud_optimized_dicom.instance import Instance

SORTING_ATTRIBUTES = {"InstanceNumber": "00200013", "SliceLocation": "00201041"}


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

    def sort_instances(self, strict=False):
        """Sort the instances dict, in the logical order they would be expected to be read (e.g. by instance number or slice location).

        If sorting is successful, set `is_sorted=True`.
        If sorting is unsuccessful (and `strict=False`), set `is_sorted=False`.

        Raises:
            ValueError: if sorting is unsuccessful and `strict=True`
        """

        def _get_sorted_metadata_uid_instance_tuples():
            metadata_uid_instance_tuples = [
                (metadata_uid, instance)
                for metadata_uid, instance in self.instances.items()
            ]
            # if there's only one instance, return it as is
            if len(metadata_uid_instance_tuples) <= 1:
                return metadata_uid_instance_tuples
            # attempt to sort by by each attribute in SORTING_ATTRIBUTES
            for tag in SORTING_ATTRIBUTES.values():
                # do not attempt sorting if any instances are missing the tag
                if any(
                    tag not in instance.metadata
                    for uid, instance in metadata_uid_instance_tuples
                ):
                    continue
                # sortable attributes are expected to be stored in metadata as "tag": {"vr":"VR","Value":[some_value]}
                return sorted(
                    metadata_uid_instance_tuples,
                    key=lambda x: x[1].metadata[tag]["Value"][0],
                )
            # if we get here, sorting failed
            msg = f"Unable to sort instances by any known sorting attributes ({', '.join(SORTING_ATTRIBUTES.keys())})"
            if strict:
                raise ValueError(msg)
            logger.warning(msg)
            return metadata_uid_instance_tuples

        # early exit if already sorted
        if self.is_sorted:
            return
        # attempt sorting
        try:
            metadata_uid_instance_tuples = _get_sorted_metadata_uid_instance_tuples()
            self.instances = {
                metadata_uid: instance
                for metadata_uid, instance in metadata_uid_instance_tuples
            }
            self.is_sorted = True
        except ValueError:
            self.is_sorted = False

    def to_dict(self) -> dict:
        # Use v2 format by default for new metadata
        study_uid_key = "deid_study_uid" if self.hashed_uids else "study_uid"
        series_uid_key = "deid_series_uid" if self.hashed_uids else "series_uid"
        base_dict = {
            study_uid_key: self.study_uid,
            series_uid_key: self.series_uid,
            "cod": {
                "instances": {
                    instance_uid: instance.to_cod_dict_v2()
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
        instances = {}
        for instance_uid, instance_dict in cod_dict.get("instances", {}).items():
            # Detect format based on version field
            version = instance_dict.get("version", None)
            if version == "2.0":
                instances[instance_uid] = Instance.from_cod_dict_v2(
                    instance_dict, uid_hash_func=uid_hash_func
                )
            elif version in ["1.0", None]:
                # The original metadata format was identical to v1, but missing the version field.
                # Therefore we try to load as v1 if the version field is missing.
                instances[instance_uid] = Instance.from_cod_dict_v1(
                    instance_dict, uid_hash_func=uid_hash_func
                )
            else:
                raise ValueError(f"Unexpected COD metadata version: {version}")

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
