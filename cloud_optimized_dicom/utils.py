DICOM_PREAMBLE = b"\x00" * 128 + b"DICM"

import io


def find_pattern(f: io.BufferedReader, pattern: bytes, buffer_size=8192):
    """
    Finds the pattern from file like object and gives index found or returns -1
    """
    assert len(pattern) < buffer_size
    size = len(pattern)
    overlap_size = size - 1
    start_position = f.tell()
    windowed_bytes = bytearray(buffer_size)

    # Read the initial buffer
    while num_bytes := f.readinto(windowed_bytes):
        # Search for the pattern in the current byte window
        index = windowed_bytes.find(pattern)
        if index != -1:
            # found the index, return the relative position
            return f.tell() - start_position - num_bytes + index

        # If the data is smaller than buffer size, this is the last
        # loop and should break.
        if num_bytes < buffer_size:
            break

        # Back seek to allow for window overlap
        f.seek(-overlap_size, 1)
    return -1
