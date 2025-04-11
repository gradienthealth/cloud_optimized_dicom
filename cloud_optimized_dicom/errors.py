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
