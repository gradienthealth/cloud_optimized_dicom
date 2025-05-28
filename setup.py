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
        "smart-open",
        "ratarmountcore",
        "numpy",
        "google-cloud-storage",
        "apache-beam[gcp]",
        "filetype",
        "pylibjpeg",
        "pylibjpeg-libjpeg",
        "pylibjpeg-openjpeg",
        "pydicom3 @ git+https://github.com/gradienthealth/pydicom-3.git",
        "opencv-python-headless",
        "ffmpeg-python",
    ],
    extras_require={
        "test": [
            "pydicom==2.3.0",
        ],
    },
)
