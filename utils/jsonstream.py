"""Stream the elements of a huge top-level JSON array without materialising them all.

Angel's scrip master has >100k rows; ``json.loads`` builds a dict per row and
peaks at hundreds of MB, which is fatal on Render Free (512 MB). Iterating with
``raw_decode`` keeps one row alive at a time, so callers can filter as they go.
"""

from __future__ import annotations

import json
from typing import Iterator


def iter_json_array(text: str) -> Iterator[object]:
    decoder = json.JSONDecoder()
    length = len(text)
    index = text.find("[")
    if index < 0:
        return
    index += 1
    while index < length:
        while index < length and text[index] in " \t\r\n,":
            index += 1
        if index >= length or text[index] == "]":
            return
        value, index = decoder.raw_decode(text, index)
        yield value
