"""Provides routines to fetch images from TIND."""

import logging
import os
from functools import cache
from math import ceil, trunc
from pathlib import Path
from typing import Any, Callable, Optional

from mokelumne.util.fetch_tind import FetchTind

logger = logging.getLogger(__name__)
"""The logger for this module."""


def base64_size(s: int | float) -> int:
    """Determine the base64-encoded size of an object given its original size.

    :param s: The original size of the object.
    :returns: The base64-encoded size of the object.
    :note: The result of this function has the same unit as its parameter.
        For example, passing 4.5 MiB will result in 6 (MiB).
    """
    return ceil((s / 3) * 4)


class ImageFetcher:
    """Provides an object that can fetch images from TIND."""

    def __init__(self, client: FetchTind, max_w: Optional[int] = None,
                 max_h: Optional[int] = None, max_size: Optional[int] = None,
                 size_transform: Callable[[int | float], int] = None):
        """Create a new ImageFetcher.

        :param FetchTind client: The TIND client object to use for fetching images.

        :param Optional[int] max_w: The maximum width for an image.
        If None is specified, images will not have a set maximum width.

        :param Optional[int] max_h: The maximum height for an image.
        If None is specified, images will not have a set maximum height.

        :param Optional[int] max_size: The maximum size for an image in bytes.
        If None is specified, images will not have a maximum size.

        :param Callable size_transform: Collable that will transform the size.
        For example, you can pass `base64_size` in to limit the size in b64 bytes.
        This is mostly useful for LLM workloads where the image will be converted.

        :note: Size is difficult to exactly calculate for lossy formats.
        The size may vary by a few KiB in either direction from your specified `max_size`.
        """

        self.client = client
        self.max_width = max_w
        self.max_height = max_h
        self.max_size = max_size
        self.size_transform = size_transform

    @cache
    def get_metadata_for_record(self, record_id: str) -> list[dict[str, Any]]:
        """Fetch the metadata for a given record."""
        return self.client.get_file_metadata(record_id)

    def fetch_one_image_for_record(self, record_id: str, index: int = 0) -> Optional[Path]:
        """Fetch a single image for a given record.

        :param str record_id: The TIND record ID containing the image to fetch.
        :param int index: The 0-based index of the image to fetch.  Defaults to 0, the first image.
        :returns: The Path representing the on-disk path of the image fetched.
        """
        record_md = self.get_metadata_for_record(record_id)
        if record_md is None or len(record_md) < index:
            return None

        file_md = record_md[index]
        target_width = curr_width = file_md.get("width", 0)
        target_height = curr_height = file_md.get("height", 0)
        curr_size = file_md.get("size", 0)
        if self.size_transform:
            curr_size = self.size_transform(curr_size)

        factors = [1.0]

        if self.max_size and curr_size > self.max_size:
            factors.append(float(self.max_size) / curr_size)

        if self.max_width and curr_width > self.max_width:
            factors.append(float(self.max_width) / curr_width)

        if self.max_height and curr_height > self.max_height:
            factors.append(float(self.max_height) / curr_height)

        scale_factor = min(factors)
        target_width = int(trunc(target_width * scale_factor))
        target_height = int(trunc(target_height * scale_factor))

        if (curr_width != target_width) or (curr_height != target_height):
            # downsample using IIIF
            path = self.client.download_image_from_record_sized(record_id,
                                                                target_width, target_height)

            # TIND resampling may cause the image to be larger than original.  recalculate.
            # in my testing, 290827 resampled to 5998x8000 went from 2.5 MB to 4.9 MB(!)
            new_size = os.stat(path).st_size
            if self.size_transform:
                new_size = self.size_transform(new_size)
            if self.max_size and new_size > self.max_size:
                # give ourselves another 5% as a cushion.
                factor = (float(self.max_size) / new_size) - 0.05
                target_width = int(trunc(target_width * factor))
                target_height = int(trunc(target_height * factor))
                # Only re-download if it actually does exceed the limit.
                path = self.client.download_image_from_record_sized(record_id,
                                                                    target_width, target_height)
        else:
            path = self.client.download_image_file(record_id)
        return Path(path)

    def fetch_images_for_record(self, record_id: str) -> list[Optional[Path]]:
        """Fetch all images for a given record.

        :param str record_id: The TIND record ID containing the images to fetch.
        :returns: A list of Paths representing the on-disk paths of all images fetched.
        """
        record_md = self.get_metadata_for_record(record_id)
        return [self.fetch_one_image_for_record(record_id, index)
                for index in range(len(record_md))]

