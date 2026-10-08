from pathlib import Path

import pytest

from streamforge_replay.zone_lookup import (
    DEFAULT_ZONE_LOOKUP,
    PINNED_ZONE_LOOKUP_SHA256,
    load_approved_zones,
)


def test_pinned_official_lookup_is_present_and_valid():
    zones = load_approved_zones()
    assert DEFAULT_ZONE_LOOKUP.is_file()
    assert PINNED_ZONE_LOOKUP_SHA256 == "1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed"
    assert zones == frozenset(range(1, 266))


def test_modified_lookup_is_rejected(tmp_path: Path):
    changed = tmp_path / "changed.csv"
    changed.write_text('LocationID,Borough,Zone,service_zone\n1,EWR,Changed,EWR\n', encoding="utf-8")
    with pytest.raises(ValueError, match="checksum mismatch"):
        load_approved_zones(changed)
