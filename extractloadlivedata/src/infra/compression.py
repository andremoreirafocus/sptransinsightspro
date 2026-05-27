import zstandard as zstd
from typing import Tuple


def compress_data(data: str) -> Tuple[bytes, str]:
    cctx = zstd.ZstdCompressor(level=3)
    compressed_data = cctx.compress(data.encode("utf-8"))
    return compressed_data, ".zst"


def decompress_data(data: bytes) -> bytes:
    dctx = zstd.ZstdDecompressor()
    decompressed_data = dctx.decompress(data)
    return decompressed_data
