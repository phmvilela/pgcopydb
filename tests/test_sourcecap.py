"""
DB-free tests for the source-capacity collector (pg_cyan/sourcecap.py).

The connection + catalog queries are stubbed; these pin the behaviour that
matters: merging the measured catalog fields into an existing source-capacity.json
without clobbering declared/cloud fields, dropping an unpaired WAL rate to stay
contract-valid, and the optional-collector degradation contract (preserve the
existing artifact, never break the walk).
"""
import json

from pg_cyan import capacity_contract as cc
from pg_cyan.sourcecap import PgCyanSourceCapacity, collect_source_capacity_from_env


def _collector(tmp_path, conns=(200, 10), wal_rate=None, connect_ok=True):
    c = PgCyanSourceCapacity(host="src", port=5432, user="u", password="p",
                             database="db", output_dir=str(tmp_path))
    c.connect = lambda: connect_ok
    c.close = lambda: None
    c.measure_connections = lambda: conns
    c.measure_wal_rate = lambda: wal_rate
    return c


def _declared(tmp_path, **fields):
    (tmp_path / "source-capacity.json").write_text(json.dumps(fields))


def test_merges_catalog_fields_and_preserves_declared(tmp_path):
    # Declared/cloud provided the read ceiling + WAL disk free.
    _declared(tmp_path, read_throughput_bytes_per_sec=2e8,
              wal_disk_free_bytes=10 ** 12, wal_generation_bytes_per_sec=1)
    c = _collector(tmp_path, conns=(300, 25), wal_rate=5_000_000.0)
    summary = c.collect()

    assert summary["status"] == "success"
    art = json.loads((tmp_path / "source-capacity.json").read_text())
    assert cc.validate("source-capacity.json", art) == []
    # Measured catalog fields present...
    assert art["max_connections"] == 300 and art["current_connections"] == 25
    # ...measured WAL rate overrode the declared placeholder (paired with free)...
    assert art["wal_generation_bytes_per_sec"] == 5_000_000.0
    # ...and declared/cloud fields survived.
    assert art["read_throughput_bytes_per_sec"] == 2e8
    assert art["wal_disk_free_bytes"] == 10 ** 12
    assert art["_contract_version"] == cc.CONTRACT_VERSION


def test_unpaired_wal_rate_is_dropped_to_stay_valid(tmp_path):
    # No declared wal_disk_free -> a measured rate can't feed the deadline.
    c = _collector(tmp_path, conns=(150, 5), wal_rate=9_000_000.0)
    assert c.collect()["status"] == "success"
    art = json.loads((tmp_path / "source-capacity.json").read_text())
    assert "wal_generation_bytes_per_sec" not in art  # omitted, not left dangling
    assert art["max_connections"] == 150
    assert cc.validate("source-capacity.json", art) == []


def test_wal_rate_kept_when_free_present_no_declared_conns(tmp_path):
    _declared(tmp_path, wal_disk_free_bytes=5 * 10 ** 11)
    c = _collector(tmp_path, conns=(100, 0), wal_rate=3_000_000.0)
    c.collect()
    art = json.loads((tmp_path / "source-capacity.json").read_text())
    assert art["wal_generation_bytes_per_sec"] == 3_000_000.0
    assert art["wal_disk_free_bytes"] == 5 * 10 ** 11


def test_connect_failure_preserves_existing_and_writes_errors(tmp_path):
    declared = {"read_throughput_bytes_per_sec": 111, "max_connections": 9}
    _declared(tmp_path, **declared)
    c = _collector(tmp_path, connect_ok=False)
    summary = c.collect()

    assert summary["status"] == "degraded"
    # The existing artifact is untouched -- it stays the planner's fallback.
    assert json.loads((tmp_path / "source-capacity.json").read_text()) == declared
    assert (tmp_path / "source-capacity-errors.json").exists()


def test_nothing_measurable_degrades(tmp_path):
    c = _collector(tmp_path, conns=None, wal_rate=None)
    summary = c.collect()
    assert summary["status"] == "degraded"
    assert not (tmp_path / "source-capacity.json").exists()
    assert (tmp_path / "source-capacity-errors.json").exists()


def test_from_env_returns_zero_even_when_degraded(tmp_path, monkeypatch):
    monkeypatch.setenv("PG_OUTPUT_DIR", str(tmp_path))
    monkeypatch.setattr(PgCyanSourceCapacity, "connect", lambda self: False)
    assert collect_source_capacity_from_env() == 0
