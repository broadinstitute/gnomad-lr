"""Closed Y1 mirror namespace shared by primary manifest/identity tools.

Refreshes are additive siblings of the legacy namespace, never arbitrary buckets
or paths. Object names remain exact cohort/contig identities; generations remain
separate mandatory fields. A prefix is not evidence an object exists.
"""
import re

LEGACY_MIRROR_PREFIX = "gs://gnomad-lr-data/y1/sources"
_REFRESH_PREFIX = re.compile(
    r"gs://gnomad-lr-data/y1/refreshes/[0-9]{8}-[0-9a-f]{12}/sources"
)


def checked_mirror_prefix(value):
    if not isinstance(value, str) or not (
        value == LEGACY_MIRROR_PREFIX or _REFRESH_PREFIX.fullmatch(value)
    ):
        raise ValueError("Rust canonical Y1 mirror contract requires legacy prefix or YYYYMMDD-<12 lowercase hex> refresh sources prefix")
    return value


def checked_generation(value):
    if (type(value) not in (str, int)
            or not re.fullmatch(r"[1-9][0-9]{0,19}", str(value))
            or int(value) > 2**64 - 1):
        raise ValueError("immutable generation must be canonical positive decimal UInt64")
    return str(value)
