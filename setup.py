from setuptools import find_packages, setup

setup(
    name="cloud-optimized-dicom",
    version="0.1.0",
    description="A library for efficiently storing and interacting with DICOM files in the cloud",
    url="TODO",
    author="Cal Nightingale",
    author_email="cal@gradienthealth.io",
    license="MIT",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "smart-open==7.0.4",
        "ratarmountcore==0.7.1",
        "numpy",
        "google-cloud-storage==2.19.0",
        "apache-beam[gcp]==2.63.0",
        "filetype==1.2.0",
        "pylibjpeg-openjpeg==2.4.0",
        "pydicom3 @ git+https://github.com/gradienthealth/pydicom-3.git",
        "opencv-python-headless==4.11.0.86",
    ],
    extras_require={
        "test": [
            "pydicom==2.3.0",
        ],
    },
)
