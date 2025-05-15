import gzip
import json
from dataclasses import dataclass, field
from io import BytesIO
from typing import Callable, Optional

from google.cloud import storage

from cloud_optimized_dicom.instance import Instance


@dataclass
class SeriesMetadata:
    """The metadata of an entire series.

    Parameters:
        study_uid (str): The study UID of this series (should match `CODObject.study_uid`)
        series_uid (str): The series UID of this series (should match `CODObject.series_uid`)
        hashed_uids (bool): Flag indicating whether the series uses de-identified UIDs.
        instances (dict[str, Instance]): Mapping of instance UID (hashed if `hashed_uids=True`) to Instance object
        custom_tags (dict): Any additional user defined data
        If loading existing metadata, this is inferred by the presence of the key `deid_study_uid` as opposed to `study_uid`.
        If creating new metadata, this is inferred by the presence/absence of `instance.uid_hash_func` for any instances that have been added.
    """

    study_uid: str
    series_uid: str
    hashed_uids: bool
    instances: dict[str, Instance] = field(default_factory=dict)
    custom_tags: dict = field(default_factory=dict)

    def _add_custom_tag(self, tag_name: str, tag_value, overwrite_existing=False):
        """Add a custom tag to the series metadata"""
        # Raise error if tag exists and we're not overwriting existing tags
        if hasattr(self.custom_tags, tag_name) and not overwrite_existing:
            raise ValueError(
                f"Metadata tag {tag_name} already exists (and overwrite_existing=False)"
            )
        self.custom_tags[tag_name] = tag_value

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

        # Treat any remaining keys as custom tags
        custom_tags = series_metadata_dict

        return cls(
            study_uid=study_uid,
            series_uid=series_uid,
            hashed_uids=is_hashed,
            instances=instances,
            custom_tags=custom_tags,
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
