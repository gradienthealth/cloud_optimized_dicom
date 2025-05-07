import importlib.metadata
import os

# monkeypatch in pydicom submodule
import sys

# Add vendored pydicom to sys.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "pydicom", "src"))

_real_version = importlib.metadata.version  # Save the real function


def fake_version(name):
    if name == "pydicom":
        return "3.0.1"
    return _real_version(name)


importlib.metadata.version = fake_version

from pydicom.dataset import *
from pydicom.encaps import encapsulate
from pydicom.errors import InvalidDicomError

# import pydicom modules that we need
from pydicom.filebase import DicomBytesIO
from pydicom.filereader import dcmread
from pydicom.tag import Tag
from pydicom.uid import generate_uid

# undo the patch now that we've imported pydicom
importlib.metadata.version = _real_version
sys.path.pop(0)

__version__ = "3.0.1"
