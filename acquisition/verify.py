"""Bounded file validation. JSON semantics belong to the Spark routing stage."""
import gzip
import hashlib
from pathlib import Path


class ValidationError(ValueError):
    pass


def inspect_gzip(path, *, compressed_limit=256 << 20,
                 expanded_limit=4 << 30, line_limit=8 << 20):
    path = Path(path)
    size = path.stat().st_size
    if size == 0 or size > compressed_limit:
        raise ValidationError("compressed_size_out_of_budget")
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1 << 20), b""):
            digest.update(block)
    expanded = lines = 0
    try:
        with gzip.open(path, "rb") as source:
            while True:
                line = source.readline(line_limit + 1)
                if not line:
                    break
                if len(line) > line_limit:
                    raise ValidationError("line_size_out_of_budget")
                expanded += len(line)
                if expanded > expanded_limit:
                    raise ValidationError("expanded_size_out_of_budget")
                lines += 1
    except (OSError, EOFError) as error:
        raise ValidationError("invalid_gzip") from error
    if lines == 0:
        raise ValidationError("empty_archive")
    return dict(compressed_bytes=size, uncompressed_bytes=expanded,
                line_count=lines, sha256=digest.hexdigest())