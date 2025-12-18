import base64
import json
import sys
from typing import Union

import zstandard

from cloud_optimized_dicom.config import logger

_MAX_CACHE_SIZE_BYTES = 5 * 1024  # 5KB


def _decompress(compressed_metadata: str) -> dict:
    """Decompress a base64-encoded zstandard-compressed JSON string into a dict."""
    compressed_bytes = base64.b64decode(compressed_metadata)
    decompressed_bytes = zstandard.decompress(compressed_bytes)
    return json.loads(decompressed_bytes.decode("utf-8"))


def _compress(metadata: dict) -> str:
    """Compress a dict into a base64-encoded zstandard-compressed JSON string."""
    json_bytes = json.dumps(metadata).encode("utf-8")
    compressed_bytes = zstandard.compress(json_bytes)
    return base64.b64encode(compressed_bytes).decode("utf-8")


class DicomMetadata:
    """Base class for DICOM metadata (uncompressed)."""

    def __init__(self, metadata: dict):
        self._metadata = metadata

    @property
    def dicom_metadata(self) -> dict:
        """Property to access the metadata as a dict."""
        if self._metadata is not None:
            return self._metadata
        # Decompress from _compressed_string if metadata was cleared after compress()
        return _decompress(self._compressed_string)

    def get_dicom_metadata(self) -> dict:
        """Get the DICOM metadata as a dict.

        Maintained for backward compatibility with existing code.
        """
        return self.dicom_metadata

    @property
    def _dicom_metadata(self) -> Union[dict, str]:
        """Access to the underlying metadata.

        Maintained for backward compatibility with code that accesses this private field.
        Returns the compressed string if compress() was called, otherwise the dict.
        """
        if hasattr(self, "_compressed_string"):
            return self._compressed_string
        return self._metadata

    def compress(self):
        """Compress the metadata.

        Note: This method mutates the object in place.
        After calling this, _dicom_metadata will return the compressed string
        and the uncompressed dict is freed to save memory.
        """
        if hasattr(self, "_compressed_string"):
            # Already compressed
            logger.warning(
                "NO-OP: DicomMetadata.compress() called on already compressed metadata"
            )
            return
        self._compressed_string = _compress(self._metadata)
        self._metadata = None  # Free the uncompressed dict to save memory

    @classmethod
    def create(cls, obj: Union[dict, str]) -> "DicomMetadata":
        """Factory method to create the appropriate DicomMetadata subclass.

        Args:
            obj: Either a dict (uncompressed metadata) or str (compressed metadata)

        Returns:
            DicomMetadata or CompressedDicomMetadata instance
        """
        if isinstance(obj, dict):
            return cls(obj)
        elif isinstance(obj, str):
            return CompressedDicomMetadata(obj)
        else:
            raise Exception(f"Unexpected obj type: {type(obj)}")


class CompressedDicomMetadata(DicomMetadata):
    """Subclass for compressed DICOM metadata with smart caching."""

    def __init__(self, compressed_metadata: str):
        # Don't call super().__init__ since we don't have uncompressed metadata yet
        self._compressed_metadata = compressed_metadata
        self._cached_metadata = None

    @property
    def dicom_metadata(self) -> dict:
        """Property to access the metadata, decompressing on demand."""
        if self._cached_metadata is not None:
            return self._cached_metadata

        metadata = _decompress(self._compressed_metadata)

        # Cache the metadata if small
        if sys.getsizeof(metadata) < _MAX_CACHE_SIZE_BYTES:
            self._cached_metadata = metadata

        return metadata

    @property
    def compressed_metadata(self) -> str:
        """Access to the compressed metadata string."""
        return self._compressed_metadata

    @property
    def _dicom_metadata(self) -> Union[str, dict]:
        """Access to the underlying metadata.

        Returns the compressed string if not cached, or the dict if cached.
        Maintained for backward compatibility.
        """
        if self._cached_metadata is not None:
            return self._cached_metadata
        return self._compressed_metadata

    def compress(self):
        """No-op for already compressed metadata.

        Logs a warning if called on already compressed data.
        """
        logger.warning(
            "NO-OP: DicomMetadata.compress() called on already compressed metadata"
        )
