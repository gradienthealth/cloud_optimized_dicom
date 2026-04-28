def test_pydicom_version():
    """
    Test pydicom version concurrency - pydicom is 2.3.0 and pydicom3 is 3.1.0
    """
    import pydicom
    import pydicom3

    assert pydicom.__version__ == "2.3.0"
    assert pydicom3.__version__ == "3.1.0"
