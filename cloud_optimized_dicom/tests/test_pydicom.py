import unittest


class TestPydicom(unittest.TestCase):
    def test_pydicom_version(self):
        """
        Test that the pydicom version is 2.3.0 and the pydicom3 version is 3.1.0.dev0
        """
        import pydicom
        import pydicom3

        self.assertEqual(pydicom.__version__, "2.3.0")
        self.assertEqual(pydicom3.__version__, "3.1.0.dev0")
