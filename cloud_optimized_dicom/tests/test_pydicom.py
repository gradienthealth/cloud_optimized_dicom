import unittest


class TestPydicom(unittest.TestCase):
    def test_pydicom_version(self):
        """
        Test pydicom version concurrency - pydicom is 2.3.0 and pydicom3 is 3.1.0
        """
        import pydicom
        import pydicom3

        self.assertEqual(pydicom.__version__, "2.3.0")
        self.assertEqual(pydicom3.__version__, "3.1.0")
