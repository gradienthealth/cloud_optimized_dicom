import tempfile

import ffmpeg
import numpy as np
from google.cloud import storage

DEFAULT_FPS = 4
DEFAULT_QUALITY = 60


# Utility functions having to do with converting a numpy array of pixel data into jpgs and mp4s
def _convert_frame_to_jpg(frame: np.ndarray) -> bytes:
    raise NotImplementedError("Not implemented")


def _convert_frames_to_mp4(
    frames: np.ndarray, fps=DEFAULT_FPS, quality=DEFAULT_QUALITY
) -> bytes:
    """Save a 3D numpy array as a video using FFmpeg and cv2."""
    frames = _standardize_channels(frames)
    video_array = np.stack(frames)
    num_frames, height, width, *_ = video_array.shape
    if video_array.ndim == 3:
        input_pix_fmt = "gray"
        output_pix_fmt = "yuv420p"
    elif video_array.ndim == 4:
        input_pix_fmt = "rgb24" if video_array.shape[3] == 3 else "rgba"
        output_pix_fmt = "yuv420p"
    else:
        raise ValueError("Invalid video dimensions.")

    crf_value = int(
        (1 - quality / 100) * 51
    )  # Map quality [0, 100] to CRF [51, 0] for x264

    # All video files need to be 8 bit files.

    with tempfile.TemporaryDirectory() as td:
        temp_file = f"{td}/tmp.mp4"
        in_process = (
            ffmpeg.input(
                "pipe:",
                format="rawvideo",
                pix_fmt=input_pix_fmt,
                s="{}x{}".format(width, height),
                r=str(fps),
            )
            .output(
                temp_file,
                pix_fmt=output_pix_fmt,
                format="mp4",
                crf=str(crf_value),
                vcodec="libx264",
            )
            .global_args("-loglevel", "error")
            .run(input=video_array.tobytes())
        )
        with open(temp_file, "rb") as vid_file:
            return vid_file.read()


def _standardize_channels(frames: list[np.ndarray]) -> list[np.ndarray]:
    raise NotImplementedError("Not implemented")
