import unittest


class TestPydicom(unittest.TestCase):
    def test_pydicom_version(self):
        """
        Test that the pydicom version is 2.3.0 and the pydicom3 version is 3.0.1
        """
        from pydicom import __version__ as locally_installed_pd_version

        from cloud_optimized_dicom.pydicom3 import __version__ as repo_pd_version

        self.assertEqual(locally_installed_pd_version, "2.3.0")
        self.assertEqual(repo_pd_version, "3.0.1")
