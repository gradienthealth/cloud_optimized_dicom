import io
import unittest

from cloud_optimized_dicom.virtual_file import VirtualFile


class TestVirtualFile(unittest.TestCase):
    def setUp(self):
        # Create a mock file with content "0123456789"
        self.mock_content = b"0123456789"
        self.master_file = io.BytesIO(self.mock_content)
        # Create virtual file from positions 2-7 (content should be "23456")
        self.virtual_file = VirtualFile(self.master_file, 2, 7)

    def tearDown(self):
        self.master_file.close()

    # Read tests
    def test_read_all(self):
        """Test reading entire virtual file"""
        self.assertEqual(self.virtual_file.read(), b"23456")

    def test_read_partial(self):
        """Test reading specific number of bytes"""
        self.assertEqual(self.virtual_file.read(2), b"23")
        self.assertEqual(self.virtual_file.read(2), b"45")

    def test_read_beyond_bounds(self):
        """Test reading more bytes than available"""
        self.assertEqual(self.virtual_file.read(1000), b"23456")

    def test_read_at_end(self):
        """Test reading when already at end of file"""
        self.virtual_file.read()  # Read everything
        self.assertEqual(self.virtual_file.read(), b"")

    # Seek tests
    def test_seek_set(self):
        """Test seeking from start of file"""
        self.virtual_file.seek(2, io.SEEK_SET)
        self.assertEqual(self.virtual_file.read(), b"456")

    def test_seek_cur(self):
        """Test seeking from current position"""
        self.virtual_file.read(2)  # Read "23"
        self.virtual_file.seek(1, io.SEEK_CUR)
        self.assertEqual(self.virtual_file.read(), b"56")

    def test_seek_end(self):
        """Test seeking from end of file"""
        self.virtual_file.seek(-2, io.SEEK_END)
        self.assertEqual(self.virtual_file.read(), b"56")

    def test_seek_beyond_end(self):
        """Test seeking beyond end of virtual file - should be allowed"""
        self.virtual_file.seek(100, io.SEEK_SET)
        self.assertEqual(self.virtual_file.tell(), 100)
        self.assertEqual(self.virtual_file.read(), b"")

    def test_seek_negative_set(self):
        """Test seeking with negative offset using SEEK_SET - should raise ValueError"""
        with self.assertRaises(ValueError):
            self.virtual_file.seek(-1, io.SEEK_SET)

    def test_seek_negative_cur(self):
        """Test seeking with negative offset using SEEK_CUR - should clamp to start"""
        self.virtual_file.read(3)  # Read "234"
        self.virtual_file.seek(-2, io.SEEK_CUR)  # Go back 2 from position 3
        self.assertEqual(self.virtual_file.read(2), b"34")

        # Test clamping to start
        self.virtual_file.read(3)  # Read "234"
        self.virtual_file.seek(-10, io.SEEK_CUR)  # Try to go before start
        self.assertEqual(self.virtual_file.tell(), 0)  # Should be clamped to start
        self.assertEqual(self.virtual_file.read(), b"23456")  # Should read from start

    def test_seek_negative_cur_clamping(self):
        """Test that SEEK_CUR with negative offset clamps to start of file"""
        self.virtual_file.read(2)  # Position is now 2
        self.virtual_file.seek(-100, io.SEEK_CUR)  # Try to seek way before start
        self.assertEqual(self.virtual_file.tell(), 0)  # Should be at start
        self.assertEqual(self.virtual_file.read(), b"23456")  # Should read everything

    def test_seek_negative_end(self):
        """Test seeking with negative offset using SEEK_END - should be allowed"""
        self.virtual_file.seek(-3, io.SEEK_END)
        self.assertEqual(self.virtual_file.read(), b"456")

    def test_read_after_seeking_beyond(self):
        """Test reading after seeking beyond end of file"""
        self.virtual_file.seek(10, io.SEEK_SET)
        self.assertEqual(self.virtual_file.read(), b"")
        self.assertEqual(self.virtual_file.read(1), b"")

    # Tell tests
    def test_tell_initial(self):
        """Test initial position"""
        self.assertEqual(self.virtual_file.tell(), 0)

    def test_tell_after_read(self):
        """Test position after reading"""
        self.virtual_file.read(2)
        self.assertEqual(self.virtual_file.tell(), 2)

    def test_tell_after_seek(self):
        """Test position after seeking"""
        self.virtual_file.seek(3, io.SEEK_SET)
        self.assertEqual(self.virtual_file.tell(), 3)

    def test_tell_at_end(self):
        """Test position at end of file"""
        self.virtual_file.read()  # Read everything
        self.assertEqual(self.virtual_file.tell(), 5)  # Virtual file is 5 bytes long

    # Context manager tests
    def test_context_manager(self):
        """Test using VirtualFile as context manager"""
        mock_file = io.BytesIO(b"0123456789")
        with VirtualFile(mock_file, 2, 7) as vf:
            self.assertEqual(vf.read(), b"23456")
        self.assertTrue(mock_file.closed)

    # Write tests
    def test_writable(self):
        """Test that VirtualFile is not writable"""
        self.assertFalse(self.virtual_file.writable())


if __name__ == "__main__":
    unittest.main()
