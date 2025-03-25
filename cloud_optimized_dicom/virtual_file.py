import io


class VirtualFile:
    """
    File pointer wrapper that enables virtual boundaries within a master file.

    Given the byte offsets of an instance within a tar, we want to be able to return a file pointer that pydicom can read.
    Because the instance is within a tar, which likely contains other files, we cannot seek to start byte and return - how will we know where the instance ends?

    This class simulates a smaller file within the master file, with virtual start and stop byte positions.

    Example: read instance.dcm (byte range 1000-2000) from within series.tar:
    ```
    with open("series.tar", "rb") as tar_file:
        start, stop = byte_offsets
        virtual_file = VirtualFile(tar_file, 1000, 2000 + 1)
        ds = pydicom.dcmread(virtual_file)
    ```
    """

    def __init__(self, master_file: io.BufferedReader, start: int, stop: int):
        self.master_file = master_file
        self.start = start
        self.stop = stop
        # Seek to start of virtual file
        self.master_file.seek(self.start)

    def read(self, size=-1):
        # If already at/past end, return empty bytes (mimics true read behavior)
        if self.master_file.tell() >= self.stop:
            return b""

        # At most we can read from the current position to the end of the virtual file
        max_read_size = self.stop - self.master_file.tell()
        if size < 0:
            size = max_read_size
        else:
            size = min(size, max_read_size)
        return self.master_file.read(size)

    def seek(self, virtual_offset, whence=io.SEEK_SET):
        # Determine the new position relative to the start of the master file
        if whence == io.SEEK_SET:
            if virtual_offset < 0:
                raise ValueError("negative seek position")
            new_position = self.start + virtual_offset
        elif whence == io.SEEK_CUR:
            new_position = self.master_file.tell() + virtual_offset
            # Clamp to start of virtual file if seeking before start with SEEK_CUR
            if new_position < self.start:
                new_position = self.start
        elif whence == io.SEEK_END:
            new_position = self.stop + virtual_offset

        return self.master_file.seek(new_position)

    def tell(self):
        return self.master_file.tell() - self.start

    def writable(self):
        """VirtualFiles do not support writing"""
        return False

    def close(self):
        # When virtual file is closed, the master file should also be closed
        self.master_file.close()

    # Context manager support
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False  # Re-raise any exceptions
