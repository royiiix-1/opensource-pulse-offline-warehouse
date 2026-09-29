"""Immutable local acquisition cache; not a substitute for HDFS publication."""
import argparse
import datetime as dt
import json
import os
from pathlib import Path
import shutil
import urllib.request
import uuid

from acquisition.manifest import source_url, input_identity
from acquisition.verify import inspect_gzip, ValidationError


def write_new_json(path, value):
    with Path(path).open("x", encoding="utf-8") as output:
        json.dump(value, output, ensure_ascii=False, indent=2)
        output.write("\n")


def budget_check(size, free, *, compressed_limit, expanded_limit, reserve):
    if type(size) is not int or size <= 0 or size > compressed_limit:
        raise ValidationError("HEAD_size_missing_or_out_of_budget")
    # Expanded content is streamed, but reserve headroom for subsequent processing.
    if free < reserve + expanded_limit + 2 * size:
        raise ValidationError("insufficient_disk_budget")


def acquire(root, date, hour, *, compressed_limit=256 << 20,
            expanded_limit=4 << 30, reserve=20 << 30, opener=urllib.request.urlopen):
    url = source_url(date, hour)
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex
    attempt = root / "attempts" / run_id
    attempt.mkdir(parents=True)
    report = dict(run_id=run_id, source_url=url, source_date=date, source_hour=hour)
    try:
        with opener(urllib.request.Request(url, method="HEAD", headers={"User-Agent": "OpenSourcePulse/0.1 (offline research portfolio)"}), timeout=30) as response:
            if response.geturl() != url:
                raise ValidationError("unexpected_redirect")
            size = int(response.headers.get("Content-Length", "0"))
        budget_check(size, shutil.disk_usage(root).free, compressed_limit=compressed_limit,
                     expanded_limit=expanded_limit, reserve=reserve)
        report["head_compressed_bytes"] = size
        candidate = attempt / "source.json.gz"
        count = 0
        with opener(urllib.request.Request(url, headers={"User-Agent": "OpenSourcePulse/0.1 (offline research portfolio)"}), timeout=30) as response, candidate.open("xb") as output:
            if response.geturl() != url:
                raise ValidationError("unexpected_redirect")
            while True:
                block = response.read(1 << 20)
                if not block:
                    break
                count += len(block)
                if count > compressed_limit or count > size:
                    raise ValidationError("GET_size_out_of_budget")
                output.write(block)
            output.flush()
            os.fsync(output.fileno())
        if count != size:
            raise ValidationError("HEAD_GET_length_mismatch")
        stats = inspect_gzip(candidate, compressed_limit=compressed_limit,
                             expanded_limit=expanded_limit)
        manifest = dict(schema_version=1, **report, **stats,
                        acquired_at=dt.datetime.now(dt.timezone.utc).isoformat())
        manifest["input_identity"] = input_identity(manifest)
        # A complete object directory is the commit unit. No partial cache object is visible.
        objects = root / "objects"
        objects.mkdir(exist_ok=True)
        target = objects / manifest["input_identity"]
        prepared = attempt / "prepared"
        prepared.mkdir()
        candidate.rename(prepared / "source.json.gz")
        write_new_json(prepared / "manifest.json", manifest)
        try:
            prepared.rename(target)
            report["status"] = "ACQUIRED"
        except OSError:
            if not target.is_dir():
                raise
            previous = json.loads((target / "manifest.json").read_text(encoding="utf-8"))
            verified = inspect_gzip(target / "source.json.gz", compressed_limit=compressed_limit,
                                    expanded_limit=expanded_limit)
            if input_identity(previous) != manifest["input_identity"] or verified != stats:
                raise ValidationError("existing_cache_corrupt")
            report["status"] = "NO_OP"
        report.update(stats, input_identity=manifest["input_identity"], object_path=str(target))
        write_new_json(attempt / "result.json", report)
        return report
    except Exception as error:
        report.update(status="FAILED", error_type=type(error).__name__, reason=str(error))
        write_new_json(attempt / "failure.json", report)
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--hour", required=True, type=int)
    parser.add_argument("--root", default="build/acquisition")
    args = parser.parse_args()
    print(json.dumps(acquire(args.root, args.date, args.hour), indent=2))


if __name__ == "__main__":
    main()