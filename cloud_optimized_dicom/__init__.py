__version__ = "0.1.0"
__author__ = "Cal Nightingale"
__credits__ = "Gradient Health"

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
