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
        "smart-open==7.1.0",
        "ratarmountcore==0.8.0",
        "numpy",
        "google-cloud-storage==2.19.0",
        "apache-beam[gcp]==2.63.0",
        "filetype==1.2.0",
    ],
)
