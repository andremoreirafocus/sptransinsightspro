import zstandard as zstd
from typing import Tuple


def compress_data(data: str) -> Tuple[bytes, str]:
    cctx = zstd.ZstdCompressor(level=3)
    data = cctx.compress(data.encode("utf-8"))
    return data, ".zst"


def decompress_data(data: bytes) -> bytes:
    dctx = zstd.ZstdDecompressor()
    data = dctx.decompress(data)
    return data
