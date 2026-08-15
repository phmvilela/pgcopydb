"""
Network collector: measure the source<->cluster path and emit ``network.json``.

The capacity-aware planner sizes the concurrency budget partly from the network:
``per_stream_bytes_per_sec`` (a measured single stream wins outright) and
``bandwidth_bytes_per_sec`` + ``rtt_ms`` (which give the bandwidth-delay product,
i.e. how many streams it takes to fill the pipe). Nothing measures them today, so
a real run either uses an operator's declared guess or drops the network term.
This stage measures them from *inside the cluster*, over the same source
connection the copy uses, and writes the ``network.json`` artifact the planner
reads (see pg_cyan/capacity.py, pg_cyan/capacity_contract.py).

What it measures:

* **RTT** -- the min of a few TCP connects to the source's Postgres port. ICMP is
  routinely blocked (Azure), so a TCP-connect handshake to the real port is the
  robust, dependency-free latency signal.
* **per-stream throughput** -- one ``COPY (SELECT ...) TO STDOUT`` of generated
  data, bytes received over elapsed seconds. This is the effective rate one COPY
  achieves end to end over this path (network- and source-read-bounded), which is
  exactly what the cost model wants for a single stream.
* **aggregate bandwidth** -- the same COPY run over N parallel streams; total
  bytes over the wall-clock window. When the link saturates before N streams the
  aggregate is < N x per-stream, so ``bandwidth/per_stream`` (the planner's
  ``network_fill`` term) reflects the real fill factor.

It is an **optional** collector and degrades without ever breaking the walk: any
probe failure leaves an existing (declared) ``network.json`` untouched -- so the
declared value remains the fallback -- writes ``network-errors.json`` for
diagnostics, and still exits 0. A successful probe overwrites ``network.json``,
so a measured value overrides a declared one (the documented merge rule).

See also: pg_cyan/capacity.py, pg_cyan/capacity_contract.py, pg_cyan/metrics.py
(the collector pattern this follows).
"""
import json
import logging
import os
import socket
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from time import perf_counter
from typing import Dict, List, Optional, Tuple

import psycopg2
from psycopg2 import Error as PgError

from pg_cyan import capacity_contract
from pg_cyan.postgres.conn import source_conn_defaults

logger = logging.getLogger(__name__)

#: Width of each generated COPY row, in bytes. Cheap for the source to emit
#: (a single ``repeat``), so the timing reflects the path, not source CPU.
_ROW_BYTES = 4096


class _CountingSink:
    """A write-only file-like that counts bytes and discards them."""

    def __init__(self) -> None:
        self.n = 0

    def write(self, data) -> int:
        self.n += len(data)
        return len(data)


class PgCyanNetworkProbe:
    """Measures the source path and writes the ``network.json`` capacity artifact."""

    OUTPUT_FILENAME = "network.json"
    ERROR_FILENAME = "network-errors.json"

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        sslmode: Optional[str] = None,
        timeout: int = 30,
        output_dir: str = ".",
        streams: int = 4,
        stream_bytes: int = 16 * 1024 * 1024,
        rtt_samples: int = 5,
    ):
        """
        Args:
            host/port/user/password/database/sslmode: source connection.
            timeout: per-connect timeout (seconds), for both TCP and psycopg2.
            output_dir: where network.json / network-errors.json are written.
            streams: parallel COPY streams for the aggregate-bandwidth probe.
            stream_bytes: approximate bytes generated per stream.
            rtt_samples: number of TCP connects; the min is taken as the RTT.
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.sslmode = sslmode
        self.timeout = timeout
        self.output_dir = output_dir
        self.streams = max(1, int(streams))
        self.stream_bytes = max(_ROW_BYTES, int(stream_bytes))
        self.rtt_samples = max(1, int(rtt_samples))
        self.errors: List[str] = []

    def _log_event(
        self, event_type: str, status: str, message: str,
        details: Optional[Dict] = None,
    ) -> None:
        event = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "network-probe",
            "event_type": event_type,
            "status": status,
            "message": message,
            "details": details or {},
        }
        logger.info(json.dumps(event))

    # ------------------------------------------------------------- measurement

    def measure_rtt(self) -> Optional[float]:
        """Min TCP-connect time to the source port, in milliseconds (or None)."""
        best: Optional[float] = None
        for _ in range(self.rtt_samples):
            try:
                t0 = perf_counter()
                with socket.create_connection((self.host, self.port), self.timeout):
                    pass
                elapsed_ms = (perf_counter() - t0) * 1000.0
                best = elapsed_ms if best is None else min(best, elapsed_ms)
            except OSError as e:
                self._log_event("measure_rtt", "warning",
                                "TCP connect for RTT failed", {"error": str(e)})
        return best

    def _open_conn(self):
        """Open a fresh psycopg2 source connection (its own stream)."""
        kwargs = dict(host=self.host, port=self.port, user=self.user,
                      password=self.password, database=self.database,
                      connect_timeout=self.timeout)
        if self.sslmode:
            kwargs["sslmode"] = self.sslmode
        return psycopg2.connect(**kwargs)

    def _copy_sql(self) -> str:
        """A COPY that streams ~``stream_bytes`` of cheaply-generated data."""
        rows = max(1, self.stream_bytes // _ROW_BYTES)
        # `rows`/`_ROW_BYTES` are our own ints -- no user input in this SQL.
        return (f"COPY (SELECT repeat('x', {_ROW_BYTES}) "
                f"FROM generate_series(1, {rows})) TO STDOUT")

    def _copy_stream(self) -> Tuple[int, float]:
        """Run one COPY stream; return (bytes_received, seconds)."""
        conn = self._open_conn()
        try:
            sink = _CountingSink()
            cur = conn.cursor()
            t0 = perf_counter()
            cur.copy_expert(self._copy_sql(), sink)
            elapsed = perf_counter() - t0
            cur.close()
            return sink.n, max(elapsed, 1e-9)
        finally:
            conn.close()

    def measure_throughput(self) -> Optional[Tuple[float, float]]:
        """
        Measure (per_stream_bytes_per_sec, bandwidth_bytes_per_sec).

        One stream gives the per-stream rate; ``streams`` parallel streams over
        their wall-clock window give the aggregate bandwidth. Returns None if the
        source connection or COPY could not run at all.
        """
        try:
            single_bytes, single_secs = self._copy_stream()
        except (PgError, OSError) as e:
            self._log_event("measure_throughput", "failure",
                            "Single-stream COPY probe failed",
                            {"error": str(e),
                             "remediation": "Check source connectivity and that the "
                                            "role can run COPY (SELECT ...) TO STDOUT"})
            self.errors.append(f"Throughput probe failed: {e}")
            return None

        per_stream = single_bytes / single_secs

        bandwidth = per_stream
        if self.streams > 1:
            try:
                wall0 = perf_counter()
                with ThreadPoolExecutor(max_workers=self.streams) as pool:
                    results = list(pool.map(lambda _: self._copy_stream(),
                                            range(self.streams)))
                wall = max(perf_counter() - wall0, 1e-9)
                total_bytes = sum(b for b, _ in results)
                bandwidth = max(total_bytes / wall, per_stream)
            except (PgError, OSError) as e:
                # Aggregate probe failed but we still have a per-stream number;
                # fall back to it rather than losing the whole measurement.
                self._log_event("measure_throughput", "warning",
                                "Parallel bandwidth probe failed; using per-stream rate",
                                {"error": str(e)})
                bandwidth = per_stream
        return per_stream, bandwidth

    # ------------------------------------------------------------------ output

    def _write_json(self, filename: str, payload: Dict) -> Optional[str]:
        path = os.path.join(self.output_dir, filename)
        try:
            os.makedirs(self.output_dir, exist_ok=True)
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2)
            return path
        except OSError as e:
            self._log_event("write_output", "failure",
                            f"Failed to write {filename}",
                            {"path": path, "error": str(e)})
            self.errors.append(f"Failed to write {filename}: {e}")
            return None

    def _build_network(self, rtt_ms, per_stream, bandwidth) -> Dict:
        payload = {
            "component": "network-probe",
            "status": "success",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "bandwidth_bytes_per_sec": round(bandwidth, 2),
            "per_stream_bytes_per_sec": round(per_stream, 2),
            "measurement": {
                "streams": self.streams,
                "stream_bytes": self.stream_bytes,
                "rtt_samples": self.rtt_samples,
            },
        }
        if rtt_ms is not None:
            payload["rtt_ms"] = round(rtt_ms, 3)
        return capacity_contract.stamp(payload)

    def probe(self) -> Dict:
        """
        Measure the path and write ``network.json`` (or degrade).

        Returns a summary dict (``status`` ∈ ``success``/``degraded``). On a
        successful throughput measurement the artifact is written (overriding any
        declared network.json). On failure nothing is overwritten -- the declared
        value, if any, stays the planner's fallback -- and an errors file is
        written. Never raises.
        """
        summary = {"status": "degraded", "component": "network-probe",
                   "output_file": None, "error_file": None, "errors": self.errors}

        rtt_ms = self.measure_rtt()
        throughput = self.measure_throughput()

        # Throughput is mandatory for a valid artifact (the contract needs a
        # bandwidth/per-stream signal; RTT alone is not enough). Without it we
        # degrade and preserve any declared network.json.
        if throughput is None:
            error_path = self._write_json(self.ERROR_FILENAME, {
                "component": "network-probe", "status": "failure",
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "errors": self.errors or ["network probe produced no throughput sample"],
                "remediation": "A declared network.json (if present) remains in effect; "
                               "the planner drops the network term otherwise.",
            })
            self._log_event("probe", "warning",
                            "Network probe degraded; kept any declared network.json",
                            {"rtt_ms": rtt_ms})
            summary["error_file"] = error_path
            return summary

        per_stream, bandwidth = throughput
        payload = self._build_network(rtt_ms, per_stream, bandwidth)

        # Guard: our own artifact must satisfy the shared contract.
        contract_errors = capacity_contract.validate(self.OUTPUT_FILENAME, payload)
        if contract_errors:
            self.errors.extend(contract_errors)
            error_path = self._write_json(self.ERROR_FILENAME, {
                "component": "network-probe", "status": "failure",
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "errors": self.errors,
                "remediation": "Measured values did not satisfy the network contract.",
            })
            self._log_event("probe", "warning",
                            "Measured network values failed contract validation",
                            {"errors": contract_errors})
            summary["error_file"] = error_path
            return summary

        output_path = self._write_json(self.OUTPUT_FILENAME, payload)
        if output_path is None:
            return summary  # write failure already recorded; stay degraded
        self._log_event("probe", "success",
                        "Measured the source network path",
                        {"rtt_ms": payload.get("rtt_ms"),
                         "per_stream_bytes_per_sec": payload["per_stream_bytes_per_sec"],
                         "bandwidth_bytes_per_sec": payload["bandwidth_bytes_per_sec"]})
        summary.update(status="success", output_file=output_path)
        return summary


def probe_network_from_env() -> int:
    """
    Run the network probe from environment/CLI settings.

    Optional collector: returns 0 on success AND on graceful degradation (a probe
    failure must not break the migration walk); 2 only on interrupt. The declared
    network.json, if any, survives a degraded run.
    """
    try:
        defaults = source_conn_defaults()
        probe = PgCyanNetworkProbe(
            host=os.getenv("PG_SOURCE_HOST", defaults["host"]),
            port=int(os.getenv("PG_SOURCE_PORT", str(defaults["port"]))),
            user=os.getenv("PG_SOURCE_USER", defaults["user"]),
            password=os.getenv("PG_SOURCE_PASSWORD", defaults["password"]),
            database=os.getenv("PG_SOURCE_DATABASE", defaults["database"]),
            sslmode=defaults["sslmode"],
            timeout=int(os.getenv("PREFLIGHT_TIMEOUT_SECONDS", "30")),
            output_dir=os.getenv("PG_OUTPUT_DIR", "."),
            streams=int(os.getenv("PGCYAN_NET_STREAMS", "4")),
            stream_bytes=int(os.getenv("PGCYAN_NET_STREAM_BYTES", str(16 * 1024 * 1024))),
            rtt_samples=int(os.getenv("PGCYAN_NET_RTT_SAMPLES", "5")),
        )
        summary = probe.probe()
        print(json.dumps(summary, indent=2, default=str))
        return 0
    except KeyboardInterrupt:
        logger.error(json.dumps({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "status": "interrupted",
            "message": "Network probe interrupted by user",
        }))
        return 2


if __name__ == "__main__":
    sys.exit(probe_network_from_env())
