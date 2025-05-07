from cloud_optimized_dicom.cod_object import CODObject
from cloud_optimized_dicom.pydicom3 import dcmread


def generate_thumbnail(cod_obj: CODObject, dirty: bool = False):
    """Generate a thumbnail for a COD object."""
    # fetch the tar, if it's not already fetched
    if cod_obj.tar_is_empty:
        cod_obj.pull_tar(dirty=dirty)

    for instance in cod_obj.get_metadata(dirty=dirty).instances.values():
        print(instance.dicom_uri)
        with instance.open() as f:
            ds = dcmread(f)
            print(ds.StudyInstanceUID)
            print(ds.SeriesInstanceUID)
            print(ds.SOPInstanceUID)
    return None
