import hashlib

import pytest

from streamforge_replay.identity import event_id


def test_event_id_uses_fixed_canonical_encoding():
    source = "ab" * 32
    expected = hashlib.sha256(f"nyc-yellow:v1:{source}:42".encode("utf-8")).hexdigest()
    assert event_id(source.upper(), 42) == expected
    assert event_id(source, 42) == event_id(source, 42)
    assert event_id(source, 42) != event_id(source, 43)


@pytest.mark.parametrize("checksum,row", [("short", 0), ("x" * 64, 0), ("ab" * 32, -1)])
def test_event_id_rejects_invalid_inputs(checksum, row):
    with pytest.raises(ValueError):
        event_id(checksum, row)

