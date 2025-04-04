from setuptools import setup

setup(
    name="cloud-optimized-dicom",
    version="0.1.0",
    description="A library for efficiently storing and interacting with DICOM files in the cloud",
    url="TODO",
    author="Cal Nightingale",
    author_email="cal@gradienthealth.io",
    license="MIT",
    packages=["cloud_optimized_dicom"],
    install_requires=[
        "pydicom",
        "smart-open",
        "ratarmountcore",
        "numpy",
        "google-cloud-storage",
        "apache-beam[gcp]",
        "filetype",
    ],
)
