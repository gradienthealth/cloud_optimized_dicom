import base64
import json
from dataclasses import dataclass
from enum import Enum
from typing import Union

import zstandard

from cloud_optimized_dicom.config import logger


class DicomMetadataState(Enum):
    """Status of an instance's DICOM metadata.
    Possible states:
    - UNPOPULATED: When the metadata has not been fetched (maybe the instance was just appended)
    - COMPRESSED: When the metadata has been fetched but not accessed
    - DECOMPRESSED: When the metadata is accessed, it gets decompressed into a dict (OR if a v1 metadata dict is fetched)
    """

    UNPOPULATED = 1
    COMPRESSED = 2
    DECOMPRESSED = 3


@dataclass
class DicomMetadata:
    """Interface for dicom metadata (e.g. tags and values).

    Parameters:
        state: DicomMetadataState - The state of the dicom metadata.
        _dicom_metadata: Union[None, str, dict] - The dicom metadata. Prefixed with `_` to indicate it is private. Access via getters and setters.
    """

    state: DicomMetadataState = DicomMetadataState.UNPOPULATED
    _dicom_metadata: Union[None, str, dict] = None

    def compress(self):
        """Compress the dicom metadata into a string.
        Raises:
            ValueError: if the state is not DECOMPRESSED
        """
        if self.state != DicomMetadataState.DECOMPRESSED:
            raise ValueError(
                f"DicomMetadata.compress() expects state to be DECOMPRESSED (got {self.state})"
            )
        json_bytes = json.dumps(self._dicom_metadata).encode("utf-8")
        compressed_bytes = zstandard.compress(json_bytes)
        compressed_base64_string = base64.b64encode(compressed_bytes).decode("utf-8")
        self._dicom_metadata = compressed_base64_string
        self.state = DicomMetadataState.COMPRESSED

    def decompress(self):
        """Decompress the dicom metadata into a dict.
        If the metadata is already decompressed, do nothing.

        Raises:
            ValueError: if the state is UNPOPULATED
        """
        if self.state == DicomMetadataState.DECOMPRESSED:
            logger.warning(
                f"NO-OP: DicomMetadata.decompress() called on decompressed metadata"
            )
            return
        if self.state == DicomMetadataState.UNPOPULATED:
            raise ValueError(
                f"DicomMetadata.decompress() called on unpopulated metadata"
            )
        compressed_bytes = base64.b64decode(self._dicom_metadata)
        decompressed_bytes = zstandard.decompress(compressed_bytes)
        self._dicom_metadata = json.loads(decompressed_bytes.decode("utf-8"))
        self.state = DicomMetadataState.DECOMPRESSED

    def get_dicom_metadata(self) -> dict:
        """Get the dicom metadata as a dict.
        If the metadata is not decompressed, decompress it.
        """
        if self.state != DicomMetadataState.DECOMPRESSED:
            self.decompress()
        return self._dicom_metadata
