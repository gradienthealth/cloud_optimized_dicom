from dataclasses import dataclass

from cloud_optimized_dicom.errors import HintMismatchError


@dataclass
class Hints:
    """Instance-related values that COD takes at face value when optimizing, but will be verified prior to state change.

    Say you have an inventory report of some dicom bucket with (uri, size, crc32c) for each dicom instance.
    If you provide a populated Hints object when creating Instances, COD can (for example) use the hint crc32c to throw out a duplicate instance.
    If the instance is new, however, COD will verify the crc32c matches the hint and raise an exception if it does not.

    Args:
        size: int - size of the instance
        crc32c: str - crc32c of the instance
        instance_uid: str - instance uid of the instance (as found in the dicom file; ds.SOPInstanceUID)
        series_uid: str - series uid of the instance (as found in the dicom file; ds.SeriesInstanceUID)
        study_uid: str - study uid of the instance (as found in the dicom file; ds.StudyInstanceUID)
    """

    size: int = None
    crc32c: str = None
    instance_uid: str = None
    series_uid: str = None
    study_uid: str = None

    def validate(
        self,
        true_size: int,
        true_crc32c: str,
        true_instance_uid: str,
        true_series_uid: str,
        true_study_uid: str,
    ):
        """Verify all provided hints against the true values.

        Raises:
            HintMismatchError if any hint was provided and does not match the true value.
        """
        if self.size is not None:
            if self.size != true_size:
                raise HintMismatchError.from_bad_hint("size", self.size, true_size)
        if self.crc32c is not None:
            if self.crc32c != true_crc32c:
                raise HintMismatchError.from_bad_hint(
                    "crc32c", self.crc32c, true_crc32c
                )
        if self.instance_uid is not None:
            if self.instance_uid != true_instance_uid:
                raise HintMismatchError.from_bad_hint(
                    "instance_uid", self.instance_uid, true_instance_uid
                )
        if self.series_uid is not None:
            if self.series_uid != true_series_uid:
                raise HintMismatchError.from_bad_hint(
                    "series_uid", self.series_uid, true_series_uid
                )
        if self.study_uid is not None:
            if self.study_uid != true_study_uid:
                raise HintMismatchError.from_bad_hint(
                    "study_uid", self.study_uid, true_study_uid
                )
