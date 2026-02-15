import zstandard as zstd

def compress_data(data):
    cctx = zstd.ZstdCompressor(level=3)
    data = cctx.compress(data.encode("utf-8"))
    return data, ".zst"