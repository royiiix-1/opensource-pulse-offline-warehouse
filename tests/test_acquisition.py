import gzip
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest

from acquisition.download import acquire, budget_check
from acquisition.manifest import source_url, require_hours
from acquisition.verify import inspect_gzip, ValidationError

URL = source_url("2024-01-01", 0)


class Reply(io.BytesIO):
    def __init__(self, data, size, url=URL):
        super().__init__(data)
        self.headers = {"Content-Length": str(size)}
        self.url = url

    def geturl(self):
        return self.url


def server(data, advertised=None, url=URL):
    def open_request(request, timeout):
        return Reply(data, len(data) if advertised is None else advertised, url)
    return open_request


class AcquisitionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.data = gzip.compress(b'{"id":"1"}\n{"id":"2"}\n', mtime=0)

    def run_acquire(self, **kwargs):
        return acquire(self.root, "2024-01-01", 0, reserve=0,
                       expanded_limit=4096, **kwargs)

    def test_url_and_input_validation(self):
        self.assertTrue(URL.endswith("-0.json.gz"))
        for date, hour in [("2024-02-30", 0), ("../x", 0), ("2024-01-01", 24),
                           ("2024-01-01", True), ("2024-1-1", 0)]:
            with self.subTest(date=date, hour=hour), self.assertRaises(ValueError):
                source_url(date, hour)

    def test_gzip_counts_checksum(self):
        path = self.root / "input.gz"
        path.write_bytes(self.data)
        result = inspect_gzip(path)
        self.assertEqual(result["line_count"], 2)
        self.assertEqual(result["sha256"], hashlib.sha256(self.data).hexdigest())

    def test_empty_truncated_crc_and_budgets(self):
        for data, options in [(b"", {}), (gzip.compress(b""), {}), (self.data[:-5], {}),
                              (self.data[:-8] + b'\x00' * 8, {}),
                              (self.data, {"expanded_limit": 1}),
                              (self.data, {"line_limit": 2}),
                              (self.data, {"compressed_limit": 1})]:
            path = self.root / "input.gz"
            path.write_bytes(data)
            with self.subTest(options=options, size=len(data)), self.assertRaises(ValidationError):
                inspect_gzip(path, **options)

    def test_bad_json_is_not_silently_lost_by_file_validator(self):
        path = self.root / "bad-json.gz"
        path.write_bytes(gzip.compress(b'broken\n\n'))
        self.assertEqual(inspect_gzip(path)["line_count"], 2)
        # Spark must quarantine these later; successful gzip validation != valid events.

    def test_idempotent_reacquisition(self):
        first = self.run_acquire(opener=server(self.data))
        second = self.run_acquire(opener=server(self.data))
        self.assertEqual(first["status"], "ACQUIRED")
        self.assertEqual(second["status"], "NO_OP")
        self.assertEqual(first["input_identity"], second["input_identity"])
        self.assertEqual(len(list((self.root / "objects").iterdir())), 1)
        self.assertEqual(len(list((self.root / "attempts").iterdir())), 2)

    def test_failed_download_not_published_and_retry(self):
        with self.assertRaises(ValidationError):
            self.run_acquire(opener=server(self.data[:-4]))
        self.assertFalse((self.root / "objects").exists())
        failure = list((self.root / "attempts").glob("*/failure.json"))
        self.assertEqual(len(failure), 1)
        self.assertEqual(json.loads(failure[0].read_text())["status"], "FAILED")
        self.assertEqual(self.run_acquire(opener=server(self.data))["status"], "ACQUIRED")
        self.assertTrue(failure[0].exists())

    def test_head_get_mismatch(self):
        for advertised in [len(self.data) - 1, len(self.data) + 1]:
            with self.subTest(advertised=advertised), self.assertRaises(ValidationError):
                self.run_acquire(opener=server(self.data, advertised))

    def test_redirect_rejected(self):
        with self.assertRaises(ValidationError):
            self.run_acquire(opener=server(self.data, url="https://example.org/"))

    def test_same_hour_changed_content_keeps_both(self):
        first = self.run_acquire(opener=server(self.data))
        second = self.run_acquire(opener=server(gzip.compress(b'{"id":"3"}\n')))
        self.assertNotEqual(first["input_identity"], second["input_identity"])
        self.assertTrue(Path(first["object_path"]).exists())
        self.assertEqual(len(list((self.root / "objects").iterdir())), 2)

    def test_corrupt_cache_cannot_noop(self):
        first = self.run_acquire(opener=server(self.data))
        (Path(first["object_path"]) / "source.json.gz").write_bytes(b"corrupt")
        with self.assertRaises(ValidationError):
            self.run_acquire(opener=server(self.data))

    def test_coverage_missing_duplicate_partial(self):
        manifests = [dict(source_date="2024-01-01", source_hour=h,
                          source_url=source_url("2024-01-01", h), sha256="a" * 64)
                     for h in range(24)]
        self.assertEqual(require_hours(manifests, "2024-01-01"), "COMPLETE")
        with self.assertRaises(ValueError):
            require_hours(manifests[:-1], "2024-01-01")
        with self.assertRaises(ValueError):
            require_hours(manifests + [manifests[0]], "2024-01-01")
        self.assertEqual(require_hours(manifests[:1], "2024-01-01", [0]), "PARTIAL")

    def test_budget_fails_closed(self):
        for size, free in [(0, 1000), (101, 1000), (10, 10)]:
            with self.subTest(size=size, free=free), self.assertRaises(ValidationError):
                budget_check(size, free, compressed_limit=100, expanded_limit=100, reserve=100)


if __name__ == "__main__":
    unittest.main()