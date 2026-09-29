import datetime as dt
import hashlib
import json
import re


def source_url(date, hour):
    if not isinstance(date, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
        raise ValueError("date must be YYYY-MM-DD")
    dt.date.fromisoformat(date)
    if type(hour) is not int or not 0 <= hour <= 23:
        raise ValueError("hour must be an integer in 0..23")
    return f"https://data.gharchive.org/{date}-{hour}.json.gz"


def input_identity(manifest):
    value = [manifest[k] for k in
             ("source_url", "source_date", "source_hour", "sha256")]
    return hashlib.sha256(json.dumps(value, separators=(",", ":")).encode()).hexdigest()


def require_hours(manifests, date, hours=range(24)):
    expected = set(hours)
    if not expected or any(type(h) is not int or not 0 <= h <= 23 for h in expected):
        raise ValueError("invalid expected hours")
    actual = {}
    for item in manifests:
        hour = item["source_hour"]
        if item["source_date"] != date or item["source_url"] != source_url(date, hour):
            raise ValueError("source mismatch")
        if hour in actual:
            raise ValueError("duplicate source hour")
        if not re.fullmatch(r"[0-9a-f]{64}", item["sha256"]):
            raise ValueError("invalid checksum")
        actual[hour] = item
    if set(actual) != expected:
        raise ValueError(f"hour coverage mismatch: missing={sorted(expected-set(actual))}")
    return "COMPLETE" if expected == set(range(24)) else "PARTIAL"