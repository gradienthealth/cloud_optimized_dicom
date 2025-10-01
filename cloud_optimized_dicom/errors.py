class CODError(Exception):
    """Base class for all COD errors."""


class LockAcquisitionError(CODError):
    """Error raised when a lock cannot be acquired."""


class LockVerificationError(CODError):
    """Error raised when a lock cannot be verified."""


class CODObjectNotFoundError(CODError):
    """Error raised when a COD object is not found and `create_if_missing=False`."""


class CleanOpOnUnlockedCODObjectError(CODError):
    """Error raised when a clean operation is attempted on an unlocked CODObject."""


class ErrorLogExistsError(CODError):
    """Exception raised on CODObject initialization when error.log already exists in the datastore"""


class TarValidationError(CODError):
    """Base class of exception for integrity check related failures"""


class TarMissingInstanceError(TarValidationError):
    """Exception raised on CODObject integrity check when the series metadata contains an instance that is not in the tar"""


class HashMismatchError(TarValidationError):
    """Exception raised on CODObject integrity check when there is a mismatch between the crc32c hash in the metadata and the one computed from the tar"""


class HintMismatchError(CODError):
    """Exception raised on Instance validation when the hints do not match the true values"""

    @classmethod
    def from_bad_hint(
        cls, field_name: str, hinted_value: str | int, found_value: str | int
    ):
        """Constructor to create a well-formatted hint mismatch error message given the field that was bad and the hinted/found values"""
        return cls(
            f"Hint mismatch for field {field_name}. Hint: {hinted_value}; Found: {found_value}"
        )


class InstanceValidationError(CODError):
    """Exception raised on Instance validation when the instance is invalid"""
