import gzip
import json
from dataclasses import asdict, dataclass, field
from io import BytesIO

from google.cloud import storage

from cloud_optimized_dicom.instance import Instance


@dataclass
class SeriesMetadata:
    """The metadata of an entire series.

    Parameters:
        study_uid (str): The study UID of this series (should match `CODObject.study_uid`)
        series_uid (str): The series UID of this series (should match `CODObject.series_uid`)
        instances (dict[str, Instance]): Mapping of instance UID to Instance object
        custom_tags (dict): Any additional user defined data
    """

    study_uid: str
    series_uid: str
    instances: dict[str, Instance] = field(default_factory=dict)
    custom_tags: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        # TODO version handling once we have a new version
        # TODO existing gradient uses "deid_{study/series}_uid"... how to reconcile?
        return {
            "study_uid": self.study_uid,
            "series_uid": self.series_uid,
            "cod": {
                "instances": {
                    instance_uid: instance.to_cod_dict_v1()
                    for instance_uid, instance in self.instances.items()
                },
            },
            "thumbnail": asdict(self.thumbnail) if self.thumbnail else None,
        }

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
            study_uid = series_metadata_dict.pop("deid_study_uid")
            series_uid = series_metadata_dict.pop("deid_series_uid")
        else:
            study_uid = series_metadata_dict.pop("study_uid")
            series_uid = series_metadata_dict.pop("series_uid")

        # Parse standard cod metadata
        cod_dict: dict = series_metadata_dict.pop("cod")
        instances = {
            instance_uid: Instance.from_cod_dict_v1(instance_dict)
            for instance_uid, instance_dict in cod_dict.get("instances", {}).items()
        }

        # Treat any remaining keys as custom tags
        if len(series_metadata_dict) > 0:
            raise NotImplementedError(
                f"Unknown metadata keys: {series_metadata_dict.keys()}"
            )

        return cls(study_uid=study_uid, series_uid=series_uid, instances=instances)

    @classmethod
    def from_bytes(cls, bytes: bytes) -> "SeriesMetadata":
        """Class method to create a SeriesMetadata object from a bytes object."""
        return cls.from_dict(json.loads(bytes))

    @classmethod
    def from_blob(cls, blob: storage.Blob) -> "SeriesMetadata":
        """Class method to create a SeriesMetadata object from a GCS blob."""
        return cls.from_bytes(blob.download_as_bytes())
