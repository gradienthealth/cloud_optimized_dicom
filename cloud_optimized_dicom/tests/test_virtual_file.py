import io

import pytest

from cloud_optimized_dicom.virtual_file import VirtualFile


@pytest.fixture
def master_file() -> io.BytesIO:
    f = io.BytesIO(b"0123456789")
    yield f
    f.close()


@pytest.fixture
def virtual_file(master_file: io.BytesIO) -> VirtualFile:
    # virtual file from positions 2-7 (content should be "23456")
    return VirtualFile(master_file, 2, 7)


# Read tests
def test_read_all(virtual_file: VirtualFile):
    """Test reading entire virtual file"""
    assert virtual_file.read() == b"23456"


def test_read_partial(virtual_file: VirtualFile):
    """Test reading specific number of bytes"""
    assert virtual_file.read(2) == b"23"
    assert virtual_file.read(2) == b"45"


def test_read_beyond_bounds(virtual_file: VirtualFile):
    """Test reading more bytes than available"""
    assert virtual_file.read(1000) == b"23456"


def test_read_at_end(virtual_file: VirtualFile):
    """Test reading when already at end of file"""
    virtual_file.read()  # Read everything
    assert virtual_file.read() == b""


# Seek tests
def test_seek_set(virtual_file: VirtualFile):
    """Test seeking from start of file"""
    virtual_file.seek(2, io.SEEK_SET)
    assert virtual_file.read() == b"456"


def test_seek_cur(virtual_file: VirtualFile):
    """Test seeking from current position"""
    virtual_file.read(2)  # Read "23"
    virtual_file.seek(1, io.SEEK_CUR)
    assert virtual_file.read() == b"56"


def test_seek_end(virtual_file: VirtualFile):
    """Test seeking from end of file"""
    virtual_file.seek(-2, io.SEEK_END)
    assert virtual_file.read() == b"56"


def test_seek_beyond_end(virtual_file: VirtualFile):
    """Test seeking beyond end of virtual file - should be allowed"""
    virtual_file.seek(100, io.SEEK_SET)
    assert virtual_file.tell() == 100
    assert virtual_file.read() == b""


def test_seek_negative_set(virtual_file: VirtualFile):
    """Test seeking with negative offset using SEEK_SET - should raise ValueError"""
    with pytest.raises(ValueError):
        virtual_file.seek(-1, io.SEEK_SET)


def test_seek_negative_cur(virtual_file: VirtualFile):
    """Test seeking with negative offset using SEEK_CUR - should clamp to start"""
    virtual_file.read(3)  # Read "234"
    virtual_file.seek(-2, io.SEEK_CUR)  # Go back 2 from position 3
    assert virtual_file.read(2) == b"34"

    # Test clamping to start
    virtual_file.read(3)  # Read "234"
    virtual_file.seek(-10, io.SEEK_CUR)  # Try to go before start
    assert virtual_file.tell() == 0  # Should be clamped to start
    assert virtual_file.read() == b"23456"  # Should read from start


def test_seek_negative_cur_clamping(virtual_file: VirtualFile):
    """Test that SEEK_CUR with negative offset clamps to start of file"""
    virtual_file.read(2)  # Position is now 2
    virtual_file.seek(-100, io.SEEK_CUR)  # Try to seek way before start
    assert virtual_file.tell() == 0  # Should be at start
    assert virtual_file.read() == b"23456"  # Should read everything


def test_seek_negative_end(virtual_file: VirtualFile):
    """Test seeking with negative offset using SEEK_END - should be allowed"""
    virtual_file.seek(-3, io.SEEK_END)
    assert virtual_file.read() == b"456"


def test_read_after_seeking_beyond(virtual_file: VirtualFile):
    """Test reading after seeking beyond end of file"""
    virtual_file.seek(10, io.SEEK_SET)
    assert virtual_file.read() == b""
    assert virtual_file.read(1) == b""


# Tell tests
def test_tell_initial(virtual_file: VirtualFile):
    """Test initial position"""
    assert virtual_file.tell() == 0


def test_tell_after_read(virtual_file: VirtualFile):
    """Test position after reading"""
    virtual_file.read(2)
    assert virtual_file.tell() == 2


def test_tell_after_seek(virtual_file: VirtualFile):
    """Test position after seeking"""
    virtual_file.seek(3, io.SEEK_SET)
    assert virtual_file.tell() == 3


def test_tell_at_end(virtual_file: VirtualFile):
    """Test position at end of file"""
    virtual_file.read()  # Read everything
    assert virtual_file.tell() == 5  # Virtual file is 5 bytes long


# Context manager tests
def test_context_manager():
    """Test using VirtualFile as context manager"""
    mock_file = io.BytesIO(b"0123456789")
    with VirtualFile(mock_file, 2, 7) as vf:
        assert vf.read() == b"23456"
    assert mock_file.closed


# Write tests
def test_writable(virtual_file: VirtualFile):
    """Test that VirtualFile is not writable"""
    assert not virtual_file.writable()
