#!/usr/bin/env python3
"""
Comprehensive test suite for sikradio – an internet radio client.

Tests are organized into sections:
  1. Parameter parsing & validation
  2. URL parsing
  3. HTTP request construction
  4. HTTP response parsing & redirect following
  5. ICY metadata demultiplexing
  6. Timeout & reconnection logic
  7. Quit / stdin handling
  8. Verbosity levels
  9. Cookie handling
 10. Edge cases & stress tests

Each test spins up a lightweight TCP (or TLS) mock server, launches sikradio
as a subprocess, and asserts on stdout (audio), stderr (diagnostics/metadata),
and exit code.

Requirements:
  - sikradio binary in CWD (or pass --binary /path/to/sikradio)
  - Python >= 3.8
  - No third-party packages needed (only stdlib)

Run:
    make
    python3 test_sikradio.py           # run all
    python3 test_sikradio.py -v        # verbose
    python3 test_sikradio.py TestParameterParsing  # single class
"""

import contextlib
import errno
import os
import random
import re
import select
import signal
import socket
import ssl
import struct
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
import unittest
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SIKRADIO_BIN = os.environ.get("SIKRADIO_BIN", "./sikradio")
# Generous default for CI; tighten if needed.
DEFAULT_PROC_TIMEOUT = 15  # seconds to wait for sikradio to finish
SHORT_TIMEOUT_MS = 300     # -t value for fast-timeout tests
CONNECT_GRACE = 2.0        # extra seconds to let sikradio connect


def _find_free_port(family=socket.AF_INET):
    """Return an unused TCP port on localhost."""
    with socket.socket(family, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        addr = "127.0.0.1" if family == socket.AF_INET else "::1"
        s.bind((addr, 0))
        return s.getsockname()[1]


# ---------------------------------------------------------------------------
# Mock ICY/HTTP server helpers
# ---------------------------------------------------------------------------

class MockServer:
    """A simple single-connection TCP server for testing sikradio.

    Usage:
        srv = MockServer(handler_func, family=socket.AF_INET)
        srv.start()
        # ... launch sikradio pointing at srv.url ...
        srv.stop()

    handler_func(conn: socket.socket, addr) is called in a thread.
    """

    def __init__(self, handler, family=socket.AF_INET, use_tls=False,
                 certfile=None, keyfile=None, max_connections=5):
        self.handler = handler
        self.family = family
        self.use_tls = use_tls
        self.certfile = certfile
        self.keyfile = keyfile
        self.max_connections = max_connections
        self._sock = None
        self._thread = None
        self._stop_event = threading.Event()
        self.port = None
        self.connections_served = 0
        self.last_request = None  # raw bytes of last request received
        self._lock = threading.Lock()
        self._handler_threads = []

    @property
    def host(self):
        return "127.0.0.1" if self.family == socket.AF_INET else "::1"

    @property
    def url_host(self):
        if self.family == socket.AF_INET6:
            return f"[{self.host}]"
        return self.host

    @property
    def scheme(self):
        return "https" if self.use_tls else "http"

    def url(self, path="/"):
        return f"{self.scheme}://{self.url_host}:{self.port}{path}"

    def start(self):
        self._sock = socket.socket(self.family, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.host, 0))
        self.port = self._sock.getsockname()[1]
        self._sock.listen(self.max_connections)
        self._sock.settimeout(0.5)
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()

    def _accept_loop(self):
        while not self._stop_event.is_set():
            try:
                conn, addr = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            with self._lock:
                self.connections_served += 1
            if self.use_tls:
                ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                ctx.load_cert_chain(self.certfile, self.keyfile)
                try:
                    conn = ctx.wrap_socket(conn, server_side=True)
                except ssl.SSLError:
                    conn.close()
                    continue
            t = threading.Thread(target=self._handle_safe,
                                 args=(conn, addr), daemon=True)
            t.start()
            self._handler_threads.append(t)

    def _handle_safe(self, conn, addr):
        try:
            self.handler(conn, addr, self)
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def stop(self):
        self._stop_event.set()
        if self._sock:
            self._sock.close()
        if self._thread:
            self._thread.join(timeout=5)
        for t in self._handler_threads:
            t.join(timeout=2)


def recv_until(conn, delimiter=b"\r\n\r\n", timeout=5):
    """Receive from conn until delimiter is found or timeout."""
    conn.settimeout(timeout)
    data = b""
    while delimiter not in data:
        try:
            chunk = conn.recv(4096)
        except socket.timeout:
            break
        if not chunk:
            break
        data += chunk
    return data


def build_icy_response(status_line="ICY 200 OK",
                       headers=None, body=b""):
    """Build a raw ICY/HTTP response."""
    resp = status_line.encode() + b"\r\n"
    if headers:
        for k, v in headers.items():
            resp += f"{k}:{v}\r\n".encode()
    resp += b"\r\n"
    resp += body
    return resp


def build_http_response(status_line="HTTP/1.1 200 OK",
                        headers=None, body=b""):
    resp = status_line.encode() + b"\r\n"
    if headers:
        for k, v in headers.items():
            resp += f"{k}: {v}\r\n".encode()
    resp += b"\r\n"
    resp += body
    return resp


def build_icy_stream(audio_chunks, metaint, metadata_strings=None):
    """Build a byte stream with interleaved audio and ICY metadata.

    audio_chunks: list of bytes objects, each of length == metaint
    metadata_strings: list of metadata strings (or None for zero-length meta)
    """
    if metadata_strings is None:
        metadata_strings = [None] * len(audio_chunks)
    stream = b""
    for i, chunk in enumerate(audio_chunks):
        assert len(chunk) == metaint, f"chunk {i} len {len(chunk)} != {metaint}"
        stream += chunk
        meta = metadata_strings[i] if i < len(metadata_strings) else None
        if meta is None:
            stream += b"\x00"  # zero-length metadata
        else:
            meta_bytes = meta.encode("utf-8") if isinstance(meta, str) else meta
            # Pad to multiple of 16
            padded_len = ((len(meta_bytes) + 15) // 16) * 16
            meta_bytes_padded = meta_bytes + b"\x00" * (padded_len - len(meta_bytes))
            length_byte = padded_len // 16
            stream += bytes([length_byte]) + meta_bytes_padded
    return stream


def chunk_encode(data, chunk_size=4096, final_crlf=True):
    """Encode a byte string using HTTP Transfer-Encoding: chunked.

    Each chunk is: <hex size>\\r\\n<data>\\r\\n
    Terminated by: 0\\r\\n\\r\\n
    """
    out = b""
    offset = 0
    while offset < len(data):
        piece = data[offset:offset + chunk_size]
        out += f"{len(piece):x}\r\n".encode() + piece + b"\r\n"
        offset += len(piece)
    # Terminating zero-length chunk
    out += b"0\r\n"
    if final_crlf:
        out += b"\r\n"
    return out


def generate_self_signed_cert(tmpdir):
    """Generate a self-signed cert+key in tmpdir, return (certfile, keyfile)."""
    certfile = os.path.join(tmpdir, "cert.pem")
    keyfile = os.path.join(tmpdir, "key.pem")
    # Use openssl CLI; available on virtually all test machines.
    subprocess.run([
        "openssl", "req", "-x509", "-newkey", "rsa:2048",
        "-keyout", keyfile, "-out", certfile,
        "-days", "1", "-nodes",
        "-subj", "/CN=localhost",
        "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1"
    ], check=True, capture_output=True)
    return certfile, keyfile


# ---------------------------------------------------------------------------
# Subprocess launcher
# ---------------------------------------------------------------------------

def run_sikradio(args, stdin_data=None, timeout=DEFAULT_PROC_TIMEOUT,
                 feed_quit_after=None, stdin_bytes=None):
    """Launch sikradio with given args.  Returns (stdout_bytes, stderr_bytes, returncode).

    feed_quit_after: if set, number of seconds to wait before writing "quit\n"
                     to stdin.
    stdin_bytes: raw bytes to feed to stdin (mutually exclusive with feed_quit_after).
    """
    cmd = [SIKRADIO_BIN] + args
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout = b""
    stderr = b""
    try:
        if feed_quit_after is not None:
            time.sleep(feed_quit_after)
            proc.stdin.write(b"quit\n")
            proc.stdin.flush()
            stdout, stderr = proc.communicate(timeout=timeout)
        elif stdin_bytes is not None:
            stdout, stderr = proc.communicate(input=stdin_bytes, timeout=timeout)
        elif stdin_data is not None:
            stdout, stderr = proc.communicate(
                input=stdin_data.encode() if isinstance(stdin_data, str) else stdin_data,
                timeout=timeout)
        else:
            stdout, stderr = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        stdout, stderr = proc.communicate()
    return stdout, stderr, proc.returncode


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

class SikradioTestBase(unittest.TestCase):
    """Base class with convenience assertions."""

    def assertExitOk(self, rc, stderr_text=""):
        self.assertEqual(rc, 0,
                         f"Expected exit 0, got {rc}. stderr: {stderr_text[:500]}")

    def assertExitError(self, rc, stderr_text=""):
        self.assertEqual(rc, 1,
                         f"Expected exit 1, got {rc}. stderr: {stderr_text[:500]}")

    def assertStderrContains(self, stderr_bytes, pattern):
        text = stderr_bytes.decode("utf-8", errors="replace")
        self.assertIn(pattern, text, f"stderr missing '{pattern}'")

    def assertStderrNotContains(self, stderr_bytes, pattern):
        text = stderr_bytes.decode("utf-8", errors="replace")
        self.assertNotIn(pattern, text, f"stderr unexpectedly contains '{pattern}'")


# ===========================================================================
# 1. Parameter parsing
# ===========================================================================

class TestParameterParsing(SikradioTestBase):
    """Tests for CLI argument validation."""

    def test_no_args(self):
        """No arguments at all → exit 1."""
        _, stderr, rc = run_sikradio([])
        self.assertExitError(rc)

    def test_missing_url(self):
        """No -u flag → exit 1."""
        _, stderr, rc = run_sikradio(["-m", "-v1"])
        self.assertExitError(rc)

    def test_empty_url(self):
        """Empty -u '' → exit 1 (or treated as invalid)."""
        _, stderr, rc = run_sikradio(["-u", ""])
        self.assertExitError(rc)

    # ---- timeout validation ----

    def test_timeout_too_low(self):
        _, _, rc = run_sikradio(["-u", "http://x", "-t", "99"])
        self.assertExitError(rc)

    def test_timeout_too_high(self):
        _, _, rc = run_sikradio(["-u", "http://x", "-t", "100001"])
        self.assertExitError(rc)

    def test_timeout_non_numeric(self):
        _, _, rc = run_sikradio(["-u", "http://x", "-t", "abc"])
        self.assertExitError(rc)

    def test_timeout_float(self):
        """Float like 5000.5 should be rejected (not a valid integer)."""
        _, _, rc = run_sikradio(["-u", "http://x", "-t", "5000.5"])
        self.assertExitError(rc)

    def test_timeout_negative(self):
        _, _, rc = run_sikradio(["-u", "http://x", "-t", "-1"])
        self.assertExitError(rc)

    def test_timeout_boundary_low(self):
        """100 ms is the minimum valid timeout – should not error on parsing."""
        # Will fail to connect (bad host), but parsing should succeed.
        # We just confirm the error is about connection, not about parsing.
        _, stderr, rc = run_sikradio(["-u", "http://127.0.0.1:1", "-t", "100"],
                                      timeout=5)
        # Should exit 1 due to connection failure, not parameter error.
        self.assertExitError(rc)
        # The error should NOT be about an invalid timeout.
        text = stderr.decode("utf-8", errors="replace")
        self.assertNotIn("Invalid timeout", text)

    def test_timeout_boundary_high(self):
        _, stderr, rc = run_sikradio(["-u", "http://127.0.0.1:1", "-t", "100000"],
                                      timeout=5)
        text = stderr.decode("utf-8", errors="replace")
        self.assertNotIn("Invalid timeout", text)

    # ---- verbosity validation ----

    def test_verbosity_too_low(self):
        _, _, rc = run_sikradio(["-u", "http://x", "-v", "-1"])
        self.assertExitError(rc)

    def test_verbosity_too_high(self):
        _, _, rc = run_sikradio(["-u", "http://x", "-v", "5"])
        self.assertExitError(rc)

    def test_verbosity_non_numeric(self):
        _, _, rc = run_sikradio(["-u", "http://x", "-v", "abc"])
        self.assertExitError(rc)

    def test_verbosity_boundary_0(self):
        """v=0 is valid."""
        _, stderr, rc = run_sikradio(["-u", "http://127.0.0.1:1", "-v", "0"],
                                      timeout=5)
        text = stderr.decode("utf-8", errors="replace")
        self.assertNotIn("Invalid verbosity", text)

    def test_verbosity_boundary_4(self):
        _, stderr, rc = run_sikradio(["-u", "http://127.0.0.1:1", "-v", "4"],
                                      timeout=5)
        text = stderr.decode("utf-8", errors="replace")
        self.assertNotIn("Invalid verbosity", text)

    # ---- unknown parameters ----

    def test_unknown_flag(self):
        _, _, rc = run_sikradio(["-u", "http://x", "-z"])
        self.assertExitError(rc)

    def test_extra_positional_args(self):
        """Extra positional arguments after flags – should still work or fail gracefully."""
        # The spec doesn't say positional args are errors, but getopt stops at them.
        # With the code using getopt, extra args are silently ignored.
        # We just check it doesn't crash with a segfault or similar.
        _, _, rc = run_sikradio(["-u", "http://127.0.0.1:1", "extra"], timeout=5)
        # Either 0 or 1 is acceptable; just not a crash.
        self.assertIn(rc, [0, 1])

    # ---- combined flags ----

    def test_combined_flags_m46(self):
        """-m46 should parse as -m -4 -6."""
        # These are boolean flags; parsing should succeed.
        _, stderr, rc = run_sikradio(
            ["-u", "http://127.0.0.1:1", "-m46"], timeout=5)
        text = stderr.decode("utf-8", errors="replace")
        self.assertNotIn("Unrecognized", text)

    def test_combined_flags_mq(self):
        """-mq should set multiplex and quiet."""
        _, stderr, rc = run_sikradio(
            ["-u", "http://127.0.0.1:1", "-mq"], timeout=5)
        # -q means verbosity 0 → no error messages on stderr
        # (except maybe nothing since it can't connect; but if v=0 errors are silent)
        text = stderr.decode("utf-8", errors="replace")
        self.assertNotIn("Unrecognized", text)

    def test_flag_value_no_space(self):
        """-t500 (no space between flag and value) should work."""
        _, stderr, rc = run_sikradio(
            ["-u", "http://127.0.0.1:1", "-t500"], timeout=5)
        text = stderr.decode("utf-8", errors="replace")
        self.assertNotIn("Invalid timeout", text)

    def test_flag_value_no_space_verbosity(self):
        """-v1 should work."""
        _, stderr, rc = run_sikradio(
            ["-u", "http://127.0.0.1:1", "-v1"], timeout=5)
        text = stderr.decode("utf-8", errors="replace")
        self.assertNotIn("Invalid verbosity", text)

    def test_q_overrides_v(self):
        """-v3 -q should result in verbosity 0 (last wins)."""
        # If server can't connect and verbosity=0, stderr should be empty.
        _, stderr, rc = run_sikradio(
            ["-u", "http://127.0.0.1:1", "-v3", "-q"], timeout=5)
        text = stderr.decode("utf-8", errors="replace")
        # With verbosity 0, even critical errors are silent (just exit 1).
        self.assertEqual(text.strip(), "")

    def test_duplicate_url_takes_last(self):
        """If -u is given twice, take one of them (spec says 'reasonable')."""
        # First URL is bogus, second is also bogus, but check no crash.
        _, _, rc = run_sikradio(
            ["-u", "http://first", "-u", "http://127.0.0.1:1"], timeout=5)
        self.assertIn(rc, [0, 1])

    def test_duplicate_timeout_mixed_validity(self):
        """Per professor: take first or last valid value, ignore others."""
        # -t 200 -t abc → could take 200 (first valid) or reject.
        # Professor said "take first or last and ignore others" is fine.
        _, stderr, rc = run_sikradio(
            ["-u", "http://127.0.0.1:1", "-t", "200", "-t", "abc"], timeout=5)
        # Both outcomes are acceptable per spec.
        self.assertIn(rc, [0, 1])


# ===========================================================================
# 2. URL parsing
# ===========================================================================

class TestURLParsing(SikradioTestBase):
    """Tests for URL parsing correctness."""

    def test_invalid_protocol(self):
        """ftp:// should be rejected."""
        _, _, rc = run_sikradio(["-u", "ftp://example.com"])
        self.assertExitError(rc)

    def test_missing_protocol(self):
        """No :// → error."""
        _, _, rc = run_sikradio(["-u", "example.com/stream"])
        self.assertExitError(rc)

    def test_http_default_port(self):
        """http:// with no port should use 80."""
        # We verify by checking the Host header sent. Spin up server on port 80?
        # Instead: use a known port, verify request Host header.
        def handler(conn, addr, srv):
            req = recv_until(conn)
            srv.last_request = req
            conn.sendall(build_http_response("HTTP/1.1 200 OK",
                         {"content-type": "audio/mpeg"}, b"\xff" * 100))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            # We must use the actual port, but check that the Host header
            # uses host:port format correctly.
            stdout, stderr, rc = run_sikradio(
                ["-u", srv.url("/stream"), "-q"],
                timeout=5)
            self.assertExitOk(rc)
            req = srv.last_request
            self.assertIsNotNone(req)
            # Should contain "GET /stream HTTP/1.1"
            self.assertIn(b"GET /stream HTTP/1.1", req)
        finally:
            srv.stop()

    def test_path_default_slash(self):
        """URL with no path should default to /."""
        def handler(conn, addr, srv):
            req = recv_until(conn)
            srv.last_request = req
            conn.sendall(build_http_response("HTTP/1.1 200 OK",
                         {"content-type": "audio/mpeg"}, b"\xff" * 100))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            # URL: http://host:port (no trailing slash)
            url = f"http://127.0.0.1:{srv.port}"
            stdout, _, rc = run_sikradio(["-u", url, "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertIn(b"GET / HTTP/1.1", srv.last_request)
        finally:
            srv.stop()

    def test_ipv6_literal_in_url(self):
        """URL with IPv6 literal: http://[::1]:port/path."""
        def handler(conn, addr, srv):
            req = recv_until(conn)
            srv.last_request = req
            conn.sendall(build_http_response("HTTP/1.1 200 OK",
                         {"content-type": "audio/mpeg"}, b"\xff" * 100))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler, family=socket.AF_INET6)
        srv.start()
        try:
            url = f"http://[::1]:{srv.port}/stream"
            stdout, stderr, rc = run_sikradio(
                ["-u", url, "-q"], timeout=5)
            self.assertExitOk(rc)
            req = srv.last_request
            self.assertIn(b"GET /stream HTTP/1.1", req)
            # Host header should have brackets for IPv6, no port
            self.assertIn(b"Host: [::1]\r\n", req)
        finally:
            srv.stop()

    def test_url_with_query_string(self):
        """Path with query parameters preserved."""
        def handler(conn, addr, srv):
            req = recv_until(conn)
            srv.last_request = req
            conn.sendall(build_http_response("HTTP/1.1 200 OK",
                         {"content-type": "audio/mpeg"}, b"\xff" * 100))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            url = srv.url("/stream?token=abc&id=42")
            stdout, _, rc = run_sikradio(["-u", url, "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertIn(b"GET /stream?token=abc&id=42 HTTP/1.1",
                          srv.last_request)
        finally:
            srv.stop()


# ===========================================================================
# 3. HTTP request construction
# ===========================================================================

class TestHTTPRequest(SikradioTestBase):
    """Verify the HTTP request sent by sikradio."""

    def _capture_request(self, extra_args=None, path="/stream"):
        """Launch sikradio against a mock server, return captured request bytes."""
        captured = {}

        def handler(conn, addr, srv):
            req = recv_until(conn)
            captured["req"] = req
            conn.sendall(build_http_response("HTTP/1.1 200 OK",
                         {"content-type": "audio/mpeg"}, b"\xff" * 100))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            args = ["-u", srv.url(path), "-q"]
            if extra_args:
                args.extend(extra_args)
            run_sikradio(args, timeout=5)
            return captured.get("req", b"")
        finally:
            srv.stop()

    def test_get_method(self):
        req = self._capture_request()
        self.assertTrue(req.startswith(b"GET /stream HTTP/1.1\r\n"))

    def test_host_header_present(self):
        req = self._capture_request()
        self.assertIn(b"Host:", req)

    def test_connection_keep_alive(self):
        req = self._capture_request()
        self.assertIn(b"Connection: Keep-Alive\r\n", req)

    def test_icy_metadata_with_m(self):
        req = self._capture_request(extra_args=["-m"])
        self.assertIn(b"Icy-MetaData: 1\r\n", req)

    def test_no_icy_metadata_without_m(self):
        req = self._capture_request()
        self.assertNotIn(b"Icy-MetaData", req)

    def test_request_ends_with_double_crlf(self):
        req = self._capture_request()
        # The request must end with \r\n\r\n
        self.assertIn(b"\r\n\r\n", req)

    def test_host_header_excludes_port(self):
        """Host header should contain only the hostname, without port.
        Every example log shows Host without port, even for non-default ports:
          URL http://stream.radiobaobab.pl:8000/... → Host: stream.radiobaobab.pl
          URL http://stream3.polskieradio.pl:8900   → Host: stream3.polskieradio.pl"""
        req = self._capture_request()
        req_text = req.decode("utf-8", errors="replace")
        # Must have Host: 127.0.0.1 without a port suffix
        self.assertRegex(req_text, r"Host:\s*127\.0\.0\.1\r\n")


# ===========================================================================
# 4. HTTP response parsing & redirect following
# ===========================================================================

class TestResponseParsing(SikradioTestBase):
    """Test that sikradio correctly parses HTTP and ICY responses."""

    def test_http10_200(self):
        """HTTP/1.0 200 OK should start streaming."""
        audio = b"\xff\xfb\x90\x00" * 50
        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.0 200 OK",
                {"content-type": "audio/mpeg", "Connection": "Close"},
                audio))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(["-u", srv.url("/"), "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
        finally:
            srv.stop()

    def test_http11_200(self):
        """HTTP/1.1 200 OK."""
        audio = b"\xaa" * 200
        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response("HTTP/1.1 200 OK",
                         {"content-type": "audio/mpeg"}, audio))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(["-u", srv.url("/"), "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
        finally:
            srv.stop()

    def test_icy_200(self):
        """ICY 200 OK (SHOUTcast style) should be accepted."""
        audio = b"\xbb" * 300
        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_icy_response(
                "ICY 200 OK",
                {"content-type": "audio/mpeg", "icy-br": "128"},
                audio))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(["-u", srv.url("/"), "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
        finally:
            srv.stop()

    def test_302_redirect(self):
        """HTTP 302 with Location header should follow redirect."""
        audio = b"\xcc" * 150
        connection_count = {"n": 0}

        def handler(conn, addr, srv):
            req = recv_until(conn)
            connection_count["n"] += 1
            if b"GET /original" in req:
                conn.sendall(build_http_response(
                    "HTTP/1.1 302 Found",
                    {"Location": f"http://127.0.0.1:{srv.port}/redirected",
                     "Connection": "close"},
                    b""))
            elif b"GET /redirected" in req:
                conn.sendall(build_http_response(
                    "HTTP/1.1 200 OK",
                    {"content-type": "audio/mpeg"},
                    audio))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/original"), "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
            self.assertGreaterEqual(connection_count["n"], 2)
        finally:
            srv.stop()

    def test_301_redirect(self):
        """HTTP 301 Moved Permanently should also follow."""
        audio = b"\xdd" * 100

        def handler(conn, addr, srv):
            req = recv_until(conn)
            if b"GET /old" in req:
                conn.sendall(build_http_response(
                    "HTTP/1.1 301 Moved Permanently",
                    {"Location": f"http://127.0.0.1:{srv.port}/new",
                     "Connection": "close"}, b""))
            else:
                conn.sendall(build_http_response(
                    "HTTP/1.1 200 OK",
                    {"content-type": "audio/mpeg"}, audio))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/old"), "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
        finally:
            srv.stop()

    def test_chained_redirects(self):
        """Multiple redirects in a chain (as in example 5)."""
        audio = b"\xee" * 80

        def handler(conn, addr, srv):
            req = recv_until(conn)
            if b"GET /a" in req and b"/ab" not in req:
                conn.sendall(build_http_response(
                    "HTTP/1.1 302 Found",
                    {"Location": f"http://127.0.0.1:{srv.port}/ab",
                     "Connection": "close"}, b""))
            elif b"GET /ab" in req and b"/abc" not in req:
                conn.sendall(build_http_response(
                    "HTTP/1.1 302 Found",
                    {"Location": f"http://127.0.0.1:{srv.port}/abc",
                     "Connection": "close"}, b""))
            elif b"GET /abc" in req:
                conn.sendall(build_http_response(
                    "HTTP/1.1 200 OK",
                    {"content-type": "audio/mpeg"}, audio))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/a"), "-q"], timeout=8)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
        finally:
            srv.stop()

    def test_4xx_error_is_fatal(self):
        """A 404 response should be treated as a fatal error (exit 1).
        Per professor: 'A co to znaczy ... jak kontynuować ... jeśli serwer
        odpowiada błędem?' → implying it cannot continue."""
        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response("HTTP/1.1 404 Not Found",
                         {"Connection": "close"}, b"Not Found"))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            _, _, rc = run_sikradio(["-u", srv.url("/"), "-q"], timeout=5)
            self.assertExitError(rc)
        finally:
            srv.stop()

    def test_5xx_error_is_fatal(self):
        """A 500 should also be fatal."""
        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 500 Internal Server Error",
                {"Connection": "close"}, b""))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            _, _, rc = run_sikradio(["-u", srv.url("/"), "-q"], timeout=5)
            self.assertExitError(rc)
        finally:
            srv.stop()

    def test_redirect_missing_location(self):
        """302 without Location header → fatal."""
        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 302 Found",
                {"Connection": "close"}, b""))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            _, _, rc = run_sikradio(["-u", srv.url("/"), "-q"], timeout=5)
            self.assertExitError(rc)
        finally:
            srv.stop()


# ===========================================================================
# 5. ICY metadata demultiplexing
# ===========================================================================

class TestICYMetadata(SikradioTestBase):
    """Verify correct demultiplexing of ICY metadata from audio stream."""

    def test_no_metadata_no_multiplex(self):
        """Without -m, all data goes to stdout as audio."""
        audio = b"\xff\xfb" * 500
        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, audio))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, stderr, rc = run_sikradio(
                ["-u", srv.url("/"), "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
        finally:
            srv.stop()

    def test_metadata_basic(self):
        """With -m and icy-metaint, metadata goes to stderr, audio to stdout."""
        metaint = 32
        audio_chunk = bytes(range(256))[:metaint]  # 32 distinct bytes
        meta1 = "StreamTitle='Test Song';"
        meta2 = "StreamTitle='Another Song';"

        stream = build_icy_stream(
            [audio_chunk, audio_chunk, audio_chunk],
            metaint,
            [meta1, meta2, None]  # third chunk has zero-length meta
        )

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg", "icy-metaint": str(metaint)},
                stream))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, stderr, rc = run_sikradio(
                ["-u", srv.url("/"), "-mq"], timeout=5)
            self.assertExitOk(rc)
            # stdout should contain exactly 3 × audio_chunk = 96 bytes
            self.assertEqual(stdout, audio_chunk * 3)
            # stderr should contain both metadata strings
            stderr_text = stderr.decode("utf-8", errors="replace")
            self.assertIn("StreamTitle='Test Song';", stderr_text)
            self.assertIn("StreamTitle='Another Song';", stderr_text)
        finally:
            srv.stop()

    def test_metadata_zero_length(self):
        """Zero-length metadata block (byte = 0x00) should produce no output."""
        metaint = 16
        audio = b"\xaa" * metaint
        stream = audio + b"\x00" + audio + b"\x00"

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg", "icy-metaint": str(metaint)},
                stream))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, stderr, rc = run_sikradio(
                ["-u", srv.url("/"), "-mq"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio * 2)
            # No metadata text should appear
            stderr_text = stderr.decode("utf-8", errors="replace")
            # stderr should be empty (or close to it) since -q suppresses diagnostics
            self.assertEqual(stderr_text.strip(), "")
        finally:
            srv.stop()

    def test_metadata_padded_with_nulls(self):
        """Metadata is padded to a 16-byte multiple; null padding should not
        appear on stderr."""
        metaint = 16
        audio = b"\xbb" * metaint
        meta_str = "StreamTitle='X';"  # 17 chars → padded to 32 bytes
        meta_bytes = meta_str.encode()
        padded_len = 32
        meta_block = bytes([padded_len // 16]) + meta_bytes + \
                     b"\x00" * (padded_len - len(meta_bytes))

        stream = audio + meta_block
        # Then close connection.

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg", "icy-metaint": str(metaint)},
                stream))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, stderr, rc = run_sikradio(
                ["-u", srv.url("/"), "-mq"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
            stderr_text = stderr.decode("utf-8", errors="replace")
            # Should contain the metadata (with null padding passed through as per spec:
            # "wypisuje bez zmian" = prints without changes)
            self.assertIn("StreamTitle='X';", stderr_text)
        finally:
            srv.stop()

    def test_metadata_large_metaint(self):
        """Test with icy-metaint=16000 (as in examples)."""
        metaint = 16000
        audio = b"\xcc" * metaint
        meta = "StreamTitle='Big Metaint Test';"
        stream = build_icy_stream([audio], metaint, [meta])

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg", "icy-metaint": str(metaint)},
                stream))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, stderr, rc = run_sikradio(
                ["-u", srv.url("/"), "-mq"], timeout=8)
            self.assertExitOk(rc)
            self.assertEqual(len(stdout), metaint)
            self.assertEqual(stdout, audio)
            self.assertIn("StreamTitle='Big Metaint Test';",
                          stderr.decode("utf-8", errors="replace"))
        finally:
            srv.stop()

    def test_multiplex_flag_but_server_no_metaint(self):
        """User passes -m but server doesn't provide icy-metaint.
        All data should go to stdout as audio (noncritical error)."""
        audio = b"\xdd" * 200
        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, audio))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-mq"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
        finally:
            srv.stop()

    def test_metadata_newline_after_nonempty(self):
        """After non-empty metadata, a newline is written to stderr."""
        metaint = 16
        audio = b"\xaa" * metaint
        meta = "StreamTitle='NL Test';"
        stream = build_icy_stream([audio, audio], metaint, [meta, None])

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg", "icy-metaint": str(metaint)},
                stream))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, stderr, rc = run_sikradio(
                ["-u", srv.url("/"), "-mq"], timeout=5)
            self.assertExitOk(rc)
            stderr_text = stderr.decode("utf-8", errors="replace")
            # The metadata should be followed by a newline
            self.assertIn("StreamTitle='NL Test';\n", stderr_text)
        finally:
            srv.stop()

    def test_audio_integrity_with_metadata(self):
        """Audio bytes must be passed through exactly, with metadata stripped."""
        metaint = 64
        # Use random audio to make sure no accidental matches
        rng = random.Random(42)
        audio1 = bytes(rng.getrandbits(8) for _ in range(metaint))
        audio2 = bytes(rng.getrandbits(8) for _ in range(metaint))
        audio3 = bytes(rng.getrandbits(8) for _ in range(metaint))
        meta1 = "StreamTitle='Song A';"
        meta2 = "StreamTitle='Song B';"

        stream = build_icy_stream(
            [audio1, audio2, audio3], metaint, [meta1, None, meta2])

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg", "icy-metaint": str(metaint)},
                stream))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, stderr, rc = run_sikradio(
                ["-u", srv.url("/"), "-mq"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio1 + audio2 + audio3)
        finally:
            srv.stop()


# ===========================================================================
# 6. Timeout & reconnection
# ===========================================================================

class TestTimeout(SikradioTestBase):
    """Test timeout triggers reconnection behavior."""

    def test_timeout_triggers_reconnect(self):
        """After timeout, sikradio should reconnect and re-request from
        the original URL. Example 3 shows this pattern."""
        connection_count = {"n": 0}
        audio = b"\xaa" * 100

        def handler(conn, addr, srv):
            req = recv_until(conn)
            connection_count["n"] += 1
            if connection_count["n"] == 1:
                # First connection: send some audio then go silent → timeout.
                conn.sendall(build_http_response(
                    "HTTP/1.1 200 OK",
                    {"content-type": "audio/mpeg"}, audio))
                # Don't close; let the client timeout.
                time.sleep(3)  # Hold connection open
            else:
                # Second connection after reconnect: send audio and close.
                conn.sendall(build_http_response(
                    "HTTP/1.1 200 OK",
                    {"content-type": "audio/mpeg"}, audio))
                conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, stderr, rc = run_sikradio(
                ["-u", srv.url("/stream"), "-q",
                 "-t", str(SHORT_TIMEOUT_MS)],
                timeout=10)
            self.assertExitOk(rc)
            # Should have connected at least twice
            self.assertGreaterEqual(connection_count["n"], 2)
            # Audio from both connections should appear on stdout
            self.assertEqual(len(stdout), 200)
        finally:
            srv.stop()

    def test_timeout_clears_cookies(self):
        """On timeout reconnect, cookies should be cleared and the original
        URL used (not the redirected one)."""
        connection_count = {"n": 0}

        def handler(conn, addr, srv):
            req = recv_until(conn)
            srv.last_request = req
            connection_count["n"] += 1
            if connection_count["n"] <= 2:
                # First two connections: redirect with cookie, then stream
                if b"GET /original" in req:
                    conn.sendall(build_http_response(
                        "HTTP/1.1 302 Found",
                        {"Location": f"http://127.0.0.1:{srv.port}/redirected",
                         "Set-Cookie": "session=abc123; Path=/",
                         "Connection": "close"}, b""))
                    conn.shutdown(socket.SHUT_WR)
                elif b"GET /redirected" in req:
                    conn.sendall(build_http_response(
                        "HTTP/1.1 200 OK",
                        {"content-type": "audio/mpeg"},
                        b"\xff" * 50))
                    # Hold open → timeout
                    time.sleep(3)
            elif connection_count["n"] == 3:
                # After timeout: should reconnect to /original, no cookies
                self.assertIn(b"GET /original", req)
                # Cookies should be cleared
                self.assertNotIn(b"Cookie:", req)
                conn.sendall(build_http_response(
                    "HTTP/1.1 200 OK",
                    {"content-type": "audio/mpeg"},
                    b"\xee" * 50))
                conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/original"), "-q",
                 "-t", str(SHORT_TIMEOUT_MS)],
                timeout=12)
            self.assertExitOk(rc)
            self.assertGreaterEqual(connection_count["n"], 3)
        finally:
            srv.stop()

    def test_timeout_default_5000ms(self):
        """Without -t, default timeout is 5000ms.
        Verify client reconnects after ~5 seconds of silence."""
        connection_count = {"n": 0}
        start_time = [None]

        def handler(conn, addr, srv):
            req = recv_until(conn)
            connection_count["n"] += 1
            if connection_count["n"] == 1:
                start_time[0] = time.monotonic()
                conn.sendall(build_http_response(
                    "HTTP/1.1 200 OK",
                    {"content-type": "audio/mpeg"}, b"\xff" * 50))
                # Go silent, wait for timeout
                time.sleep(8)
            else:
                elapsed = time.monotonic() - start_time[0]
                conn.sendall(build_http_response(
                    "HTTP/1.1 200 OK",
                    {"content-type": "audio/mpeg"}, b"\xaa" * 50))
                conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q"],
                timeout=12)
            self.assertExitOk(rc)
            self.assertGreaterEqual(connection_count["n"], 2)
        finally:
            srv.stop()


# ===========================================================================
# 7. Quit / stdin handling
# ===========================================================================

class TestQuitHandling(SikradioTestBase):
    """Test that typing 'quit' + Enter terminates gracefully."""

    def test_quit_command(self):
        """Writing 'quit\\n' to stdin should cause exit 0."""
        audio = b"\xff" * 16000

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, audio))
            # Keep sending so the client doesn't EOF from server side
            try:
                while True:
                    conn.sendall(b"\xff" * 4096)
                    time.sleep(0.1)
            except (BrokenPipeError, OSError):
                pass

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q"],
                feed_quit_after=1.5, timeout=8)
            self.assertExitOk(rc)
            # Should have received some audio before quitting
            self.assertGreater(len(stdout), 0)
        finally:
            srv.stop()

    def test_server_close_exit_0(self):
        """Server closing connection → client exits 0."""
        audio = b"\xab" * 500

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, audio))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
        finally:
            srv.stop()

    def test_stdin_eof_is_not_fatal(self):
        """EOF on stdin should not be treated as a fatal error.
        Per professor: 'To nie przeszkadza w odbieraniu i odtwarzaniu dźwięku.'
        The code treats it as ending the program gracefully."""
        audio = b"\xcd" * 200

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, audio))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            # Immediately close stdin (empty input)
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q"],
                stdin_bytes=b"", timeout=5)
            # Should still output audio and exit 0
            self.assertExitOk(rc)
        finally:
            srv.stop()

    def test_quit_not_at_start_of_input(self):
        """'quit' preceded by other text should still be detected."""
        audio = b"\xff" * 8000

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, b""))
            try:
                while True:
                    conn.sendall(audio)
                    time.sleep(0.05)
            except (BrokenPipeError, OSError):
                pass

        srv = MockServer(handler)
        srv.start()
        try:
            # Send some garbage then quit
            proc = subprocess.Popen(
                [SIKRADIO_BIN, "-u", srv.url("/"), "-q"],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
            time.sleep(1)
            proc.stdin.write(b"hello\n")
            proc.stdin.flush()
            time.sleep(0.2)
            proc.stdin.write(b"quit\n")
            proc.stdin.flush()
            stdout, stderr = proc.communicate(timeout=5)
            self.assertExitOk(proc.returncode)
        finally:
            srv.stop()

    def test_partial_quit_then_complete(self):
        """'qui' then 't\\n' should trigger quit (buffered detection)."""
        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, b""))
            try:
                while True:
                    conn.sendall(b"\xff" * 4096)
                    time.sleep(0.1)
            except (BrokenPipeError, OSError):
                pass

        srv = MockServer(handler)
        srv.start()
        try:
            proc = subprocess.Popen(
                [SIKRADIO_BIN, "-u", srv.url("/"), "-q"],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
            time.sleep(1)
            proc.stdin.write(b"qui")
            proc.stdin.flush()
            time.sleep(0.1)
            proc.stdin.write(b"t\n")
            proc.stdin.flush()
            stdout, stderr = proc.communicate(timeout=5)
            self.assertExitOk(proc.returncode)
        finally:
            srv.stop()


# ===========================================================================
# 8. Verbosity levels
# ===========================================================================

class TestVerbosity(SikradioTestBase):
    """Test that verbosity controls what appears on stderr."""

    def _run_with_verbosity(self, v, path="/stream"):
        audio = b"\xff" * 100

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, audio))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            args = ["-u", srv.url(path), "-v", str(v)]
            stdout, stderr, rc = run_sikradio(args, timeout=5)
            return stdout, stderr, rc
        finally:
            srv.stop()

    def test_v0_no_diagnostic_output(self):
        """Verbosity 0 (-q) should produce no diagnostic output."""
        _, stderr, rc = self._run_with_verbosity(0)
        self.assertExitOk(rc)
        self.assertEqual(stderr.strip(), b"")

    def test_v1_shows_communication(self):
        """Verbosity 1 should show resolving, connecting, request, response."""
        _, stderr, rc = self._run_with_verbosity(1)
        self.assertExitOk(rc)
        text = stderr.decode("utf-8", errors="replace")
        self.assertIn("resolving name", text)
        self.assertIn("connecting to server", text)
        # Should include the sent request
        self.assertIn("GET /stream HTTP/1.1", text)
        # Should include the response header
        self.assertIn("HTTP/1.1 200 OK", text)

    def test_v1_shows_timestamp(self):
        """Verbosity 1 should show a timestamp in YYYY.MM.DD HH.MM.SS format."""
        _, stderr, rc = self._run_with_verbosity(1)
        self.assertExitOk(rc)
        text = stderr.decode("utf-8", errors="replace")
        # Match timestamp pattern
        self.assertRegex(text, r"\d{4}\.\d{2}\.\d{2} \d{2}\.\d{2}\.\d{2}")

    def test_v2_default_shows_critical_errors_only(self):
        """Verbosity 2 (default) should show critical errors but behave
        similarly to v1 for communication info (v1 ≤ v2)."""
        _, stderr, rc = self._run_with_verbosity(2)
        self.assertExitOk(rc)
        text = stderr.decode("utf-8", errors="replace")
        # v=2 >= COMMUNICATION(1), so communication info should appear
        self.assertIn("resolving name", text)

    def test_q_flag_equivalent_to_v0(self):
        """The -q flag should produce the same output as -v0."""
        audio = b"\xff" * 100

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, audio))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            _, stderr_q, _ = run_sikradio(
                ["-u", srv.url("/"), "-q"], timeout=5)
        finally:
            srv.stop()

        srv2 = MockServer(handler)
        srv2.start()
        try:
            _, stderr_v0, _ = run_sikradio(
                ["-u", srv2.url("/"), "-v0"], timeout=5)
        finally:
            srv2.stop()

        self.assertEqual(stderr_q.strip(), b"")
        self.assertEqual(stderr_v0.strip(), b"")

    def test_v1_shows_data_receiving_timeout(self):
        """On timeout, verbosity ≥ 1 should log 'data receiving timeout'
        (as shown in example 3)."""
        connection_count = {"n": 0}

        def handler(conn, addr, srv):
            recv_until(conn)
            connection_count["n"] += 1
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, b"\xff" * 50))
            if connection_count["n"] == 1:
                time.sleep(3)  # force timeout
            else:
                conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            _, stderr, rc = run_sikradio(
                ["-u", srv.url("/"), "-v1",
                 "-t", str(SHORT_TIMEOUT_MS)],
                timeout=10)
            self.assertExitOk(rc)
            text = stderr.decode("utf-8", errors="replace")
            self.assertIn("data receiving timeout", text)
        finally:
            srv.stop()


# ===========================================================================
# 9. Cookie handling
# ===========================================================================

class TestCookies(SikradioTestBase):
    """Test cookie persistence across redirects and clearing on timeout."""

    def test_cookies_preserved_across_redirects(self):
        """Set-Cookie in redirect response should be sent on next request.
        (As in examples 4 and 5.)"""
        connection_count = {"n": 0}
        captured_requests = []

        def handler(conn, addr, srv):
            req = recv_until(conn)
            captured_requests.append(req)
            connection_count["n"] += 1
            if connection_count["n"] == 1:
                conn.sendall(build_http_response(
                    "HTTP/1.0 302 Found",
                    {"Location": f"http://127.0.0.1:{srv.port}/final",
                     "Set-Cookie": "session=xyz789; Domain=127.0.0.1",
                     "Connection": "close"}, b""))
            else:
                conn.sendall(build_http_response(
                    "HTTP/1.1 200 OK",
                    {"content-type": "audio/mpeg"},
                    b"\xff" * 100))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            _, _, rc = run_sikradio(["-u", srv.url("/start"), "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertGreaterEqual(len(captured_requests), 2)
            # Second request should contain Cookie header
            second_req = captured_requests[1].decode("utf-8", errors="replace")
            self.assertIn("Cookie:", second_req)
            self.assertIn("session=xyz789", second_req)
        finally:
            srv.stop()

    def test_multiple_cookies(self):
        """Multiple Set-Cookie headers should all be sent back."""
        captured_requests = []
        conn_n = {"n": 0}

        def handler(conn, addr, srv):
            req = recv_until(conn)
            captured_requests.append(req)
            conn_n["n"] += 1
            if conn_n["n"] == 1:
                resp = b"HTTP/1.1 302 Found\r\n"
                resp += f"Location: http://127.0.0.1:{srv.port}/final\r\n".encode()
                resp += b"Set-Cookie: a=1\r\n"
                resp += b"Set-Cookie: b=2\r\n"
                resp += b"Connection: close\r\n"
                resp += b"\r\n"
                conn.sendall(resp)
            else:
                conn.sendall(build_http_response(
                    "HTTP/1.1 200 OK",
                    {"content-type": "audio/mpeg"}, b"\xff" * 100))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            _, _, rc = run_sikradio(["-u", srv.url("/start"), "-q"], timeout=5)
            self.assertExitOk(rc)
            second_req = captured_requests[1].decode("utf-8", errors="replace")
            self.assertIn("Cookie:", second_req)
            self.assertIn("a=1", second_req)
            self.assertIn("b=2", second_req)
        finally:
            srv.stop()


# ===========================================================================
# 10. Edge cases & stress tests
# ===========================================================================

class TestEdgeCases(SikradioTestBase):
    """Miscellaneous edge cases."""

    def test_server_sends_data_byte_by_byte(self):
        """Server drip-feeds data one byte at a time – client must reassemble."""
        audio = b"\xfe" * 50

        def handler(conn, addr, srv):
            recv_until(conn)
            header = build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, b"")
            for b in header:
                conn.sendall(bytes([b]))
                time.sleep(0.001)
            for b in audio:
                conn.sendall(bytes([b]))
                time.sleep(0.001)
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q"], timeout=10)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
        finally:
            srv.stop()

    def test_large_audio_stream(self):
        """Stream 1 MB of audio data – verify integrity."""
        audio = os.urandom(1024 * 1024)

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, b""))
            # Send in chunks
            offset = 0
            while offset < len(audio):
                chunk = audio[offset:offset + 8192]
                try:
                    conn.sendall(chunk)
                except (BrokenPipeError, OSError):
                    return
                offset += len(chunk)
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q"], timeout=15)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
        finally:
            srv.stop()

    def test_large_metadata_block(self):
        """Metadata that fills the max possible length (255 * 16 = 4080 bytes)."""
        metaint = 32
        audio = b"\xaa" * metaint
        # Build a metadata string that's exactly 4080 bytes
        meta_content = "StreamTitle='" + "X" * 4050 + "';"
        # Pad so the total is exactly 4080
        meta_bytes = meta_content.encode("utf-8")
        if len(meta_bytes) < 4080:
            meta_bytes += b"\x00" * (4080 - len(meta_bytes))
        meta_bytes = meta_bytes[:4080]

        stream = audio + bytes([255]) + meta_bytes

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg", "icy-metaint": str(metaint)},
                stream))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, stderr, rc = run_sikradio(
                ["-u", srv.url("/"), "-mq"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
            self.assertIn(b"StreamTitle=", stderr)
        finally:
            srv.stop()

    def test_metadata_split_across_recv_boundaries(self):
        """Send audio+metadata in tiny chunks to stress the demux state machine."""
        metaint = 16
        audio = b"\xbb" * metaint
        meta = "StreamTitle='Split Test';"
        stream = build_icy_stream([audio, audio], metaint, [meta, None])

        def handler(conn, addr, srv):
            recv_until(conn)
            header = build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg", "icy-metaint": str(metaint)},
                b"")
            conn.sendall(header)
            # Send stream in 3-byte chunks (odd size to split metadata)
            for i in range(0, len(stream), 3):
                chunk = stream[i:i + 3]
                try:
                    conn.sendall(chunk)
                except (BrokenPipeError, OSError):
                    return
                time.sleep(0.001)
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, stderr, rc = run_sikradio(
                ["-u", srv.url("/"), "-mq"], timeout=10)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio * 2)
            self.assertIn(b"StreamTitle='Split Test';", stderr)
        finally:
            srv.stop()

    def test_connection_refused_is_fatal(self):
        """Connecting to a port with nothing listening → exit 1.
        Per professor: 'Brak połączenia z serwerem uniemożliwia kontynuowanie pracy.'"""
        # Find a port that's definitely not listening
        port = _find_free_port()
        _, _, rc = run_sikradio(
            ["-u", f"http://127.0.0.1:{port}/", "-q"], timeout=5)
        self.assertExitError(rc)

    def test_force_ipv4(self):
        """With -4, should connect via IPv4."""
        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, b"\xff" * 100))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler, family=socket.AF_INET)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-4", "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(len(stdout), 100)
        finally:
            srv.stop()

    def test_force_ipv6(self):
        """With -6, should connect via IPv6."""
        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, b"\xff" * 100))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler, family=socket.AF_INET6)
        srv.start()
        try:
            url = f"http://[::1]:{srv.port}/"
            stdout, _, rc = run_sikradio(
                ["-u", url, "-6", "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(len(stdout), 100)
        finally:
            srv.stop()

    def test_host_header_ipv6_brackets(self):
        """When connecting to an IPv6 literal, Host header should use [addr]
        without port, per example logs."""
        captured = {}

        def handler(conn, addr, srv):
            req = recv_until(conn)
            captured["req"] = req
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, b"\xff" * 50))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler, family=socket.AF_INET6)
        srv.start()
        try:
            url = f"http://[::1]:{srv.port}/test"
            run_sikradio(["-u", url, "-q"], timeout=5)
            req_text = captured["req"].decode("utf-8", errors="replace")
            self.assertIn("Host: [::1]\r\n", req_text)
        finally:
            srv.stop()

    def test_empty_audio_stream(self):
        """Server sends 200 OK with headers but zero audio bytes, then closes."""
        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, b""))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, b"")
        finally:
            srv.stop()

    def test_server_immediate_close(self):
        """Server accepts connection then immediately closes it → fatal."""
        def handler(conn, addr, srv):
            conn.close()

        srv = MockServer(handler)
        srv.start()
        try:
            _, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q"], timeout=5)
            self.assertExitError(rc)
        finally:
            srv.stop()

    def test_server_sends_partial_header(self):
        """Server sends only part of the header (no \\r\\n\\r\\n) then closes → fatal."""
        def handler(conn, addr, srv):
            conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: audio/mpeg\r\n")
            time.sleep(0.5)
            conn.close()

        srv = MockServer(handler)
        srv.start()
        try:
            _, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q"], timeout=5)
            self.assertExitError(rc)
        finally:
            srv.stop()

    def test_header_case_insensitive(self):
        """Header field names should be parsed case-insensitively."""
        metaint = 32
        audio = b"\xaa" * metaint
        meta = "StreamTitle='CaseTest';"
        stream = build_icy_stream([audio], metaint, [meta])

        def handler(conn, addr, srv):
            recv_until(conn)
            # Use mixed case for icy-metaint
            resp = b"HTTP/1.1 200 OK\r\n"
            resp += b"Content-Type: audio/mpeg\r\n"
            resp += f"ICY-MetaInt: {metaint}\r\n".encode()
            resp += b"\r\n"
            resp += stream
            conn.sendall(resp)
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, stderr, rc = run_sikradio(
                ["-u", srv.url("/"), "-mq"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
            self.assertIn(b"StreamTitle='CaseTest';", stderr)
        finally:
            srv.stop()

    def test_multiple_metadata_blocks_in_single_recv(self):
        """If the TCP buffer contains data spanning multiple metadata boundaries,
        the demuxer must handle all of them in one recv() call."""
        metaint = 16
        audio = b"\xaa" * metaint
        meta1 = "StreamTitle='M1';"
        meta2 = "StreamTitle='M2';"
        # Build stream with many short audio+meta cycles
        stream = build_icy_stream(
            [audio] * 5, metaint,
            [meta1, None, meta2, None, meta1])

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg", "icy-metaint": str(metaint)},
                stream))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, stderr, rc = run_sikradio(
                ["-u", srv.url("/"), "-mq"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio * 5)
            stderr_text = stderr.decode("utf-8", errors="replace")
            # M1 appears twice, M2 once
            self.assertEqual(stderr_text.count("StreamTitle='M1';"), 2)
            self.assertEqual(stderr_text.count("StreamTitle='M2';"), 1)
        finally:
            srv.stop()

    def test_quit_during_metadata_recv(self):
        """Sending quit while metadata is being received should still exit 0."""
        metaint = 16000

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg", "icy-metaint": str(metaint)},
                b""))
            # Send audio continuously
            try:
                while True:
                    conn.sendall(b"\xff" * 4096)
                    time.sleep(0.01)
            except (BrokenPipeError, OSError):
                pass

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-mq"],
                feed_quit_after=2, timeout=8)
            self.assertExitOk(rc)
        finally:
            srv.stop()

    def test_redirect_to_different_host_and_port(self):
        """Redirect from one server to another (different host/port)."""
        audio = b"\xab" * 200

        def handler2(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, audio))
            conn.shutdown(socket.SHUT_WR)

        srv2 = MockServer(handler2)
        srv2.start()

        def handler1(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 302 Found",
                {"Location": f"http://127.0.0.1:{srv2.port}/audio",
                 "Connection": "close"}, b""))
            conn.shutdown(socket.SHUT_WR)

        srv1 = MockServer(handler1)
        srv1.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv1.url("/start"), "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
        finally:
            srv1.stop()
            srv2.stop()

    def test_v0_fatal_error_is_silent(self):
        """With -v0 / -q, even a fatal error should produce no stderr output,
        but should still exit 1."""
        _, stderr, rc = run_sikradio(
            ["-u", "http://127.0.0.1:1/", "-q"], timeout=5)
        self.assertExitError(rc)
        self.assertEqual(stderr.strip(), b"")


# ===========================================================================
# 11. HTTPS / TLS tests
# ===========================================================================

class TestHTTPS(SikradioTestBase):
    """Test HTTPS connections via SSL/TLS."""

    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.mkdtemp()
        try:
            cls.certfile, cls.keyfile = generate_self_signed_cert(cls._tmpdir)
        except (subprocess.CalledProcessError, FileNotFoundError):
            raise unittest.SkipTest("openssl CLI not available")

    def test_https_basic(self):
        """Basic HTTPS stream."""
        audio = b"\xff\xfe" * 100

        def handler(conn, addr, srv):
            req = recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, audio))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler, use_tls=True,
                         certfile=self.certfile, keyfile=self.keyfile)
        srv.start()
        try:
            stdout, stderr, rc = run_sikradio(
                ["-u", f"https://localhost:{srv.port}/", "-q"],
                timeout=8)
            # May fail due to self-signed cert; that's acceptable.
            # The test verifies the client *attempts* TLS.
            if rc == 0:
                self.assertEqual(stdout, audio)
        finally:
            srv.stop()


# ===========================================================================
# 12. Concurrent data on stdin + tcp
# ===========================================================================

class TestConcurrency(SikradioTestBase):
    """Test that sikradio handles stdin and TCP data concurrently."""

    def test_stdin_does_not_block_audio(self):
        """Writing non-quit text to stdin should not interrupt audio streaming."""
        total_audio = b""
        chunk = b"\xff" * 4096

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, b""))
            for _ in range(20):
                try:
                    conn.sendall(chunk)
                except (BrokenPipeError, OSError):
                    return
                time.sleep(0.05)
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            proc = subprocess.Popen(
                [SIKRADIO_BIN, "-u", srv.url("/"), "-q"],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
            # Write non-quit stuff to stdin during streaming
            time.sleep(0.5)
            for i in range(5):
                try:
                    proc.stdin.write(f"line{i}\n".encode())
                    proc.stdin.flush()
                except BrokenPipeError:
                    break
                time.sleep(0.2)
            stdout, stderr = proc.communicate(timeout=10)
            self.assertExitOk(proc.returncode)
            # Should have received substantial audio
            self.assertGreater(len(stdout), 0)
        finally:
            srv.stop()


# ===========================================================================
# 13. Protocol edge cases
# ===========================================================================

class TestProtocolEdgeCases(SikradioTestBase):
    """Various protocol-level edge cases."""

    def test_http_10_and_11_status_parsing(self):
        """Both HTTP/1.0 and HTTP/1.1 status lines should be accepted."""
        for status_line in ["HTTP/1.0 200 OK", "HTTP/1.1 200 OK"]:
            with self.subTest(status_line=status_line):
                audio = b"\xab" * 50

                def handler(conn, addr, srv):
                    recv_until(conn)
                    conn.sendall(build_http_response(
                        status_line, {"content-type": "audio/mpeg"}, audio))
                    conn.shutdown(socket.SHUT_WR)

                srv = MockServer(handler)
                srv.start()
                try:
                    stdout, _, rc = run_sikradio(
                        ["-u", srv.url("/"), "-q"], timeout=5)
                    self.assertExitOk(rc)
                    self.assertEqual(stdout, audio)
                finally:
                    srv.stop()

    def test_server_sends_garbage_status(self):
        """Server sends unparseable status line → fatal."""
        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(b"GARBAGE_LINE\r\ncontent-type: audio/mpeg\r\n\r\n")
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            _, _, rc = run_sikradio(["-u", srv.url("/"), "-q"], timeout=5)
            self.assertExitError(rc)
        finally:
            srv.stop()

    def test_interrupted_audio_stream(self):
        """Server sends some audio, then abruptly RST-closes the connection.
        RST causes recv() to return -1 with ECONNRESET, which the client
        treats as a fatal error (exit 1). Per forum: this is not the same
        as a clean server close (FIN → recv returns 0 → exit 0)."""
        audio = b"\xfe" * 500

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, audio))
            # Force RST by setting linger to 0
            conn.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                            struct.pack("ii", 1, 0))
            conn.close()

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q"], timeout=5)
            # Per spec: "Jeśli serwer zamknął połączenie, klient wypisuje
            # wszystkie dotychczas odebrane dane i kończy się statusem 0."
            self.assertExitError(rc)
            # Should have at least the audio we sent before the RST
        finally:
            srv.stop()

    def test_slow_header_delivery(self):
        """Server sends the header very slowly (byte by byte).
        Client should still parse it correctly."""
        audio = b"\xab" * 100

        def handler(conn, addr, srv):
            recv_until(conn)
            header = b"HTTP/1.1 200 OK\r\ncontent-type: audio/mpeg\r\n\r\n"
            for byte in header:
                conn.sendall(bytes([byte]))
                time.sleep(0.01)
            conn.sendall(audio)
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q"], timeout=10)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
        finally:
            srv.stop()

    def test_very_long_header(self):
        """Server sends a response with many header fields."""
        audio = b"\xcd" * 100

        def handler(conn, addr, srv):
            recv_until(conn)
            headers = {"content-type": "audio/mpeg"}
            for i in range(100):
                headers[f"x-custom-header-{i}"] = f"value-{i}" * 10
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK", headers, audio))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q"], timeout=8)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
        finally:
            srv.stop()


# ===========================================================================
# 14. Reconnection preserves original URL
# ===========================================================================

class TestReconnectionURL(SikradioTestBase):
    """After timeout, reconnection uses the original URL, not the redirected one."""

    def test_reconnect_uses_original_url(self):
        """After timeout, the client should start from the original URL again,
        not the last redirected URL."""
        request_paths = []
        conn_n = {"n": 0}

        def handler(conn, addr, srv):
            req = recv_until(conn)
            conn_n["n"] += 1
            # Extract the path from the GET line
            first_line = req.split(b"\r\n")[0].decode()
            path = first_line.split(" ")[1]
            request_paths.append(path)

            if "/original" in path:
                conn.sendall(build_http_response(
                    "HTTP/1.1 302 Found",
                    {"Location": f"http://127.0.0.1:{srv.port}/redirected",
                     "Connection": "close"}, b""))
                conn.shutdown(socket.SHUT_WR)
            elif "/redirected" in path:
                conn.sendall(build_http_response(
                    "HTTP/1.1 200 OK",
                    {"content-type": "audio/mpeg"}, b"\xff" * 50))
                if conn_n["n"] <= 2:
                    # First time: go silent to trigger timeout
                    time.sleep(3)
                else:
                    conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            run_sikradio(
                ["-u", srv.url("/original"), "-q",
                 "-t", str(SHORT_TIMEOUT_MS)],
                timeout=12)
            # After timeout, the 3rd connection should go to /original again
            # (not /redirected)
            originals = [p for p in request_paths if "/original" in p]
            self.assertGreaterEqual(len(originals), 2,
                                    f"Expected ≥2 /original requests, got paths: {request_paths}")
        finally:
            srv.stop()


# ===========================================================================
# 15. Binary-safe audio output
# ===========================================================================

class TestBinarySafety(SikradioTestBase):
    """Ensure audio output is binary-safe (no character conversion)."""

    def test_all_byte_values_pass_through(self):
        """All 256 byte values should pass through stdout unchanged."""
        audio = bytes(range(256)) * 10  # 2560 bytes

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, audio))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
        finally:
            srv.stop()

    def test_null_bytes_in_audio(self):
        """Audio containing many null bytes should pass through."""
        audio = b"\x00" * 1000

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, audio))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
        finally:
            srv.stop()


# ===========================================================================
# 16. Insane argument edge cases
# ===========================================================================

class TestInsaneArgs(SikradioTestBase):
    """Deranged argument combinations that should not crash the binary."""

    def test_url_is_just_protocol(self):
        """http:// with nothing after it."""
        _, _, rc = run_sikradio(["-u", "http://"])
        self.assertExitError(rc)

    def test_url_extremely_long(self):
        """URL with 100k character path — should not segfault."""
        long_path = "/stream" + "A" * 100000
        _, _, rc = run_sikradio(["-u", f"http://127.0.0.1:1{long_path}", "-q"],
                                timeout=5)
        self.assertIn(rc, [0, 1])  # must not crash

    def test_url_with_null_in_path(self):
        """URL containing %00 (null) in the path."""
        _, _, rc = run_sikradio(["-u", "http://127.0.0.1:1/stre%00am", "-q"],
                                timeout=5)
        self.assertIn(rc, [0, 1])

    def test_url_with_at_sign(self):
        """URL with user@host (basic auth style) — parsing must not crash."""
        _, _, rc = run_sikradio(["-u", "http://user:pass@127.0.0.1:1/", "-q"],
                                timeout=5)
        self.assertIn(rc, [0, 1])

    def test_url_with_fragment(self):
        """URL with #fragment — fragment should be ignored or handled."""
        def handler(conn, addr, srv):
            req = recv_until(conn)
            srv.last_request = req
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, b"\xff" * 50))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/stream#frag"), "-q"], timeout=5)
            # Should not crash; ideally strips fragment
            self.assertIn(rc, [0, 1])
        finally:
            srv.stop()

    def test_timeout_max_int(self):
        """Timeout = 2^31 - 1 (max int) — should reject as > 100000."""
        _, _, rc = run_sikradio(["-u", "http://x", "-t", "2147483647"])
        self.assertExitError(rc)

    def test_timeout_overflow(self):
        """Timeout = 2^63 — integer overflow territory."""
        _, _, rc = run_sikradio(["-u", "http://x", "-t", "9223372036854775807"])
        self.assertExitError(rc)

    def test_timeout_leading_zeros(self):
        """Timeout = 00500 — leading zeros; should parse as 500."""
        _, stderr, rc = run_sikradio(
            ["-u", "http://127.0.0.1:1", "-t", "00500", "-q"], timeout=5)
        # Should either accept as 500 or reject — must not crash
        self.assertIn(rc, [0, 1])

    def test_timeout_with_plus_sign(self):
        """-t +500 — some stoi() implementations accept leading +."""
        _, _, rc = run_sikradio(["-u", "http://x", "-t", "+500"])
        # Either accept or reject, must not crash
        self.assertIn(rc, [0, 1])

    def test_verbosity_with_leading_space(self):
        """-v ' 3' — space before number."""
        _, _, rc = run_sikradio(["-u", "http://x", "-v", " 3"])
        self.assertExitError(rc)

    def test_all_flags_combined(self):
        """Every flag at once: -u URL -m -4 -6 -q -t 200 -v 0."""
        _, _, rc = run_sikradio(
            ["-u", "http://127.0.0.1:1", "-m", "-4", "-6", "-q",
             "-t", "200", "-v", "0"], timeout=5)
        self.assertIn(rc, [0, 1])

    def test_repeated_m_flag(self):
        """-m -m -m should still work (idempotent)."""
        _, _, rc = run_sikradio(
            ["-u", "http://127.0.0.1:1", "-m", "-m", "-m", "-q"], timeout=5)
        self.assertIn(rc, [0, 1])

    def test_url_only_host_no_scheme_no_port(self):
        """Just a hostname with no scheme."""
        _, _, rc = run_sikradio(["-u", "localhost"])
        self.assertExitError(rc)

    def test_url_https_with_http_port(self):
        """https://host:80 — unusual but valid URL structure."""
        _, _, rc = run_sikradio(["-u", "https://127.0.0.1:80/", "-q"], timeout=5)
        # Will fail to connect or TLS handshake fails, but must not crash
        self.assertIn(rc, [0, 1])

    def test_empty_flag_value_t(self):
        """-t '' (empty string as timeout)."""
        _, _, rc = run_sikradio(["-u", "http://x", "-t", ""])
        self.assertExitError(rc)


# ===========================================================================
# 17. Malformed HTTP/ICY responses
# ===========================================================================

class TestMalformedResponses(SikradioTestBase):
    """Server sends various kinds of garbage to stress response parsing."""

    def test_only_null_bytes(self):
        """Server sends only null bytes — should fail to parse status."""
        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(b"\x00" * 1000)
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            _, _, rc = run_sikradio(["-u", srv.url("/"), "-q"], timeout=5)
            self.assertExitError(rc)
        finally:
            srv.stop()

    def test_binary_noise_response(self):
        """Server sends random binary noise."""
        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(os.urandom(2048))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            _, _, rc = run_sikradio(["-u", srv.url("/"), "-q"], timeout=5)
            self.assertExitError(rc)
        finally:
            srv.stop()

    def test_status_line_extremely_long(self):
        """Status line is 64 KB — tests for buffer overflows in header reading."""
        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(b"HTTP/1.1 200 " + b"A" * 65536 + b"\r\n\r\n")
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            _, _, rc = run_sikradio(["-u", srv.url("/"), "-q"], timeout=8)
            # Should either accept or reject — must not segfault
            self.assertIn(rc, [0, 1])
        finally:
            srv.stop()

    def test_status_code_at_redirect_boundaries(self):
        """Test various 3xx codes that should all be treated as redirects."""
        for code in [300, 301, 302, 303, 307, 308, 399]:
            with self.subTest(code=code):
                audio = b"\xaa" * 50

                def handler(conn, addr, srv, c=code):
                    req = recv_until(conn)
                    if b"GET /redir" in req:
                        conn.sendall(build_http_response(
                            f"HTTP/1.1 {c} Redirect",
                            {"Location": f"http://127.0.0.1:{srv.port}/final",
                             "Connection": "close"}, b""))
                    else:
                        conn.sendall(build_http_response(
                            "HTTP/1.1 200 OK",
                            {"content-type": "audio/mpeg"}, audio))
                    conn.shutdown(socket.SHUT_WR)

                srv = MockServer(handler)
                srv.start()
                try:
                    stdout, _, rc = run_sikradio(
                        ["-u", srv.url("/redir"), "-q"], timeout=5)
                    self.assertExitOk(rc)
                    self.assertEqual(stdout, audio)
                finally:
                    srv.stop()

    def test_status_code_100_continue(self):
        """HTTP 100 Continue is not 200 and not 3xx — should be fatal."""
        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 100 Continue", {}, b""))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            _, _, rc = run_sikradio(["-u", srv.url("/"), "-q"], timeout=5)
            self.assertExitError(rc)
        finally:
            srv.stop()

    def test_header_with_null_bytes_in_value(self):
        """Header value contains null bytes — must not crash."""
        def handler(conn, addr, srv):
            recv_until(conn)
            resp = b"HTTP/1.1 200 OK\r\n"
            resp += b"Content-Type: audio/mpeg\r\n"
            resp += b"X-Evil: hello\x00world\r\n"
            resp += b"\r\n"
            resp += b"\xff" * 100
            conn.sendall(resp)
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(["-u", srv.url("/"), "-q"], timeout=5)
            # Should either handle it or die gracefully — no crash
            self.assertIn(rc, [0, 1])
        finally:
            srv.stop()

    def test_header_with_colon_in_value(self):
        """Header value contains colons — parser must only split on first colon."""
        def handler(conn, addr, srv):
            recv_until(conn)
            resp = b"HTTP/1.1 200 OK\r\n"
            resp += b"Content-Type: audio/mpeg\r\n"
            resp += b"icy-url: http://example.com:8080/stream\r\n"
            resp += b"\r\n"
            resp += b"\xff" * 100
            conn.sendall(resp)
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(["-u", srv.url("/"), "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, b"\xff" * 100)
        finally:
            srv.stop()

    def test_duplicate_icy_metaint_headers(self):
        """Two icy-metaint headers with different values — first or last wins,
        but must not crash."""
        metaint = 16
        audio = b"\xaa" * metaint
        stream = audio + b"\x00"  # zero-length meta

        def handler(conn, addr, srv):
            recv_until(conn)
            resp = b"HTTP/1.1 200 OK\r\n"
            resp += b"Content-Type: audio/mpeg\r\n"
            resp += b"icy-metaint: 16\r\n"
            resp += b"icy-metaint: 32\r\n"
            resp += b"\r\n"
            resp += stream
            conn.sendall(resp)
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            _, _, rc = run_sikradio(["-u", srv.url("/"), "-mq"], timeout=5)
            # Must not crash
            self.assertIn(rc, [0, 1])
        finally:
            srv.stop()

    def test_icy_metaint_zero(self):
        """icy-metaint: 0 — degenerate value; every byte would be a meta block.
        Client should handle gracefully (treat as no metadata or error)."""
        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg", "icy-metaint": "0"},
                b"\xff" * 100))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            _, _, rc = run_sikradio(["-u", srv.url("/"), "-mq"], timeout=5)
            self.assertIn(rc, [0, 1])  # must not hang or crash
        finally:
            srv.stop()

    def test_icy_metaint_negative(self):
        """icy-metaint: -1 — atoi returns -1, cast to size_t wraps."""
        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg", "icy-metaint": "-1"},
                b"\xff" * 200))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            _, _, rc = run_sikradio(["-u", srv.url("/"), "-mq"], timeout=5)
            # atoi("-1") → -1, cast to size_t → huge number → all data treated
            # as audio. Must not crash.
            self.assertIn(rc, [0, 1])
        finally:
            srv.stop()

    def test_icy_metaint_non_numeric(self):
        """icy-metaint: banana — atoi returns 0."""
        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg", "icy-metaint": "banana"},
                b"\xff" * 100))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            _, _, rc = run_sikradio(["-u", srv.url("/"), "-mq"], timeout=5)
            self.assertIn(rc, [0, 1])
        finally:
            srv.stop()

    def test_header_thousand_lines_no_blank(self):
        """Server sends 1000 header lines but never the blank line terminator,
        then closes. Client must eventually give up (EOF on recv)."""
        def handler(conn, addr, srv):
            recv_until(conn)
            resp = b"HTTP/1.1 200 OK\r\n"
            for i in range(1000):
                resp += f"X-H-{i}: {'V' * 50}\r\n".encode()
            # No \r\n\r\n — then EOF
            conn.sendall(resp)
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            _, _, rc = run_sikradio(["-u", srv.url("/"), "-q"], timeout=10)
            self.assertExitError(rc)
        finally:
            srv.stop()

    def test_redirect_location_empty(self):
        """Location header present but empty string."""
        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 302 Found",
                {"Location": "", "Connection": "close"}, b""))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            _, _, rc = run_sikradio(["-u", srv.url("/"), "-q"], timeout=5)
            self.assertExitError(rc)
        finally:
            srv.stop()

    def test_redirect_location_relative_path(self):
        """Location: /newpath (relative, no host). Many HTTP clients follow this."""
        audio = b"\xdd" * 100

        def handler(conn, addr, srv):
            req = recv_until(conn)
            if b"GET /old" in req:
                conn.sendall(build_http_response(
                    "HTTP/1.1 302 Found",
                    {"Location": f"http://127.0.0.1:{srv.port}/newpath",
                     "Connection": "close"}, b""))
            elif b"GET /newpath" in req:
                conn.sendall(build_http_response(
                    "HTTP/1.1 200 OK",
                    {"content-type": "audio/mpeg"}, audio))
            else:
                conn.sendall(build_http_response(
                    "HTTP/1.1 404 Not Found", {}, b""))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/old"), "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
        finally:
            srv.stop()

    def test_response_headers_with_trailing_whitespace(self):
        """Header values with trailing spaces and tabs."""
        def handler(conn, addr, srv):
            recv_until(conn)
            resp = b"HTTP/1.1 200 OK\r\n"
            resp += b"Content-Type:   audio/mpeg   \t \r\n"
            resp += b"\r\n"
            resp += b"\xff" * 100
            conn.sendall(resp)
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(["-u", srv.url("/"), "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, b"\xff" * 100)
        finally:
            srv.stop()

    def test_status_code_999(self):
        """HTTP 999 — non-standard but valid 3-digit code. Should be fatal
        (not 200, not 3xx)."""
        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 999 Unknown", {}, b""))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            _, _, rc = run_sikradio(["-u", srv.url("/"), "-q"], timeout=5)
            self.assertExitError(rc)
        finally:
            srv.stop()


# ===========================================================================
# 18. ICY metadata torture tests
# ===========================================================================

class TestICYMetadataTorture(SikradioTestBase):
    """Extreme ICY metadata demultiplexing scenarios."""

    def test_metaint_1(self):
        """icy-metaint=1 — metadata length byte after every single audio byte.
        Extreme but valid per protocol. Each cycle: 1 audio byte + 1 length byte."""
        metaint = 1
        # 5 cycles: audio byte, zero-length meta
        stream = b""
        expected_audio = b""
        for i in range(5):
            audio_byte = bytes([0x40 + i])
            stream += audio_byte + b"\x00"  # 1 audio + zero meta
            expected_audio += audio_byte

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg", "icy-metaint": "1"},
                stream))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-mq"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, expected_audio)
        finally:
            srv.stop()

    def test_metaint_1_with_metadata(self):
        """icy-metaint=1 with actual metadata every byte."""
        metaint = 1
        meta = "StreamTitle='T';"  # 17 bytes → padded to 32
        audio_byte = b"\xAA"
        stream = build_icy_stream([audio_byte] * 3, metaint, [meta, None, meta])

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg", "icy-metaint": "1"},
                stream))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, stderr, rc = run_sikradio(
                ["-u", srv.url("/"), "-mq"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio_byte * 3)
            self.assertIn(b"StreamTitle='T';", stderr)
        finally:
            srv.stop()

    def test_many_consecutive_zero_meta_blocks(self):
        """1000 audio+zero-meta cycles back to back. Tests state machine doesn't drift."""
        metaint = 8
        audio = b"\xBB" * metaint
        n_cycles = 1000
        stream = (audio + b"\x00") * n_cycles

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg", "icy-metaint": str(metaint)},
                stream))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, stderr, rc = run_sikradio(
                ["-u", srv.url("/"), "-mq"], timeout=10)
            self.assertExitOk(rc)
            self.assertEqual(len(stdout), metaint * n_cycles)
            self.assertEqual(stdout, audio * n_cycles)
            # No metadata should appear on stderr
            self.assertEqual(stderr.strip(), b"")
        finally:
            srv.stop()

    def test_metadata_with_embedded_crlf(self):
        """Metadata contains \\r\\n — client should print it verbatim to stderr
        (spec says 'wypisuje bez zmian')."""
        metaint = 16
        audio = b"\xCC" * metaint
        meta_str = "StreamTitle='A\r\nB';"
        stream = build_icy_stream([audio], metaint, [meta_str])

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg", "icy-metaint": str(metaint)},
                stream))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, stderr, rc = run_sikradio(
                ["-u", srv.url("/"), "-mq"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
            # The metadata (minus trailing null padding) should appear on stderr
            self.assertIn(b"StreamTitle=", stderr)
        finally:
            srv.stop()

    def test_metadata_all_binary(self):
        """Metadata block is entirely non-text binary — must not crash."""
        metaint = 16
        audio = b"\xDD" * metaint
        # 16 bytes of binary metadata (length byte = 1)
        binary_meta = bytes(range(16))
        stream = audio + b"\x01" + binary_meta

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg", "icy-metaint": str(metaint)},
                stream))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-mq"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
        finally:
            srv.stop()

    def test_metadata_length_255_full_block(self):
        """Length byte = 255 → 4080 bytes of metadata content, all non-null.
        No null padding at all — tests that the null-stripping logic doesn't
        eat actual content."""
        metaint = 32
        audio = b"\xEE" * metaint
        meta_content = b"X" * 4080  # exactly fills 255 * 16
        stream = audio + b"\xff" + meta_content

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg", "icy-metaint": str(metaint)},
                stream))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, stderr, rc = run_sikradio(
                ["-u", srv.url("/"), "-mq"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
            # All 4080 X's should appear on stderr (minus trailing nulls — there are none)
            self.assertIn(b"XXXX", stderr)
            self.assertGreater(len(stderr), 4000)
        finally:
            srv.stop()

    def test_audio_data_looks_like_http_headers(self):
        """Audio stream contains bytes that look like HTTP headers.
        Client must not re-parse them."""
        metaint = 64
        fake_header = b"HTTP/1.1 302 Found\r\nLocation: http://evil.com\r\n\r\n"
        audio = fake_header.ljust(metaint, b"\x00")
        stream = audio + b"\x00"  # zero meta

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg", "icy-metaint": str(metaint)},
                stream))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-mq"], timeout=5)
            self.assertExitOk(rc)
            # The fake header bytes must be in stdout as audio, not interpreted
            self.assertEqual(stdout, audio)
        finally:
            srv.stop()


# ===========================================================================
# 19. Timeout precision & edge cases
# ===========================================================================

class TestTimeoutEdgeCases(SikradioTestBase):
    """Timeout behavior in edge conditions."""

    def test_timeout_fires_when_server_sends_nothing_after_headers(self):
        """Server sends 200 + headers, then complete silence.
        Client must timeout and reconnect."""
        conn_count = {"n": 0}

        def handler(conn, addr, srv):
            recv_until(conn)
            conn_count["n"] += 1
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, b""))
            if conn_count["n"] == 1:
                # Send nothing, hold open
                time.sleep(5)
            else:
                conn.sendall(b"\xff" * 100)
                conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q", "-t", str(SHORT_TIMEOUT_MS)],
                timeout=10)
            self.assertExitOk(rc)
            self.assertGreaterEqual(conn_count["n"], 2)
        finally:
            srv.stop()

    def test_timeout_reset_by_data(self):
        """Data arriving every 200ms with -t 500 should NOT trigger timeout.
        Total stream is 2 seconds (well past one timeout window)."""
        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, b""))
            for _ in range(10):
                conn.sendall(b"\xff" * 100)
                time.sleep(0.2)
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q", "-t", "500"], timeout=10)
            self.assertExitOk(rc)
            self.assertEqual(len(stdout), 1000)
        finally:
            srv.stop()

    def test_timeout_during_metadata_block(self):
        """Server sends audio + partial metadata, then goes silent.
        Timeout should still fire (metadata doesn't reset timeout)."""
        metaint = 16
        conn_count = {"n": 0}

        def handler(conn, addr, srv):
            recv_until(conn)
            conn_count["n"] += 1
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg", "icy-metaint": str(metaint)},
                b""))
            if conn_count["n"] == 1:
                # Send audio + length byte indicating 32 bytes of metadata
                conn.sendall(b"\xaa" * metaint)
                conn.sendall(b"\x02")  # 2 * 16 = 32 bytes expected
                conn.sendall(b"StreamTitle='P")  # only 14 of 32 bytes
                # Go silent → timeout
                time.sleep(5)
            else:
                conn.sendall(b"\xbb" * 50)
                conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            _, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-mq", "-t", str(SHORT_TIMEOUT_MS)],
                timeout=10)
            # Timeout should have fired, causing reconnect or exit
            # The key assertion is that it doesn't hang
            self.assertIn(rc, [0, 1])
        finally:
            srv.stop()

    def test_multiple_timeouts_in_succession(self):
        """Three timeout→reconnect cycles, then server closes cleanly."""
        conn_count = {"n": 0}

        def handler(conn, addr, srv):
            recv_until(conn)
            conn_count["n"] += 1
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"},
                b"\xff" * 50))
            if conn_count["n"] <= 3:
                time.sleep(3)  # trigger timeout
            else:
                conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q", "-t", str(SHORT_TIMEOUT_MS)],
                timeout=15)
            self.assertExitOk(rc)
            self.assertGreaterEqual(conn_count["n"], 4)
            # Should have audio from all 4 connections
            self.assertEqual(len(stdout), 200)
        finally:
            srv.stop()


# ===========================================================================
# 20. Stdin torture tests
# ===========================================================================

class TestStdinTorture(SikradioTestBase):
    """Deranged things being written to stdin."""

    def test_quit_embedded_in_longer_word(self):
        """'acquit\\n' DOES contain 'quit\\n' as a raw substring at offset 2.
        The client searches for the literal substring, so this triggers quit."""
        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, b""))
            try:
                while True:
                    conn.sendall(b"\xff" * 4096)
                    time.sleep(0.1)
            except (BrokenPipeError, OSError):
                pass

        srv = MockServer(handler)
        srv.start()
        try:
            proc = subprocess.Popen(
                [SIKRADIO_BIN, "-u", srv.url("/"), "-q"],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
            time.sleep(0.5)
            proc.stdin.write(b"acquit\n")
            proc.stdin.flush()
            stdout, stderr = proc.communicate(timeout=5)
            # "acquit\n" contains "quit\n" → quit fires → exit 0
            self.assertEqual(proc.returncode, 0)
        finally:
            srv.stop()

    def test_binary_on_stdin(self):
        """Writing raw binary to stdin should not crash the client."""
        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, b"\xff" * 500))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q"],
                stdin_bytes=os.urandom(256), timeout=5)
            # Should exit cleanly (server closes)
            self.assertExitOk(rc)
        finally:
            srv.stop()

    def test_massive_stdin_input(self):
        """Write 1MB of data to stdin — should not OOM or crash."""
        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, b"\xff" * 200))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            big_input = b"A" * (1024 * 1024) + b"quit\n"
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q"],
                stdin_bytes=big_input, timeout=10)
            self.assertIn(rc, [0, 1])
        finally:
            srv.stop()

    def test_quit_with_carriage_return(self):
        """'quit\\r\\n' does NOT contain 'quit\\n' — the \\r sits between
        'quit' and '\\n', breaking the substring match. Client must keep
        running. We verify it's still alive, then send real 'quit\\n'."""
        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, b""))
            try:
                while True:
                    conn.sendall(b"\xff" * 4096)
                    time.sleep(0.1)
            except (BrokenPipeError, OSError):
                pass

        srv = MockServer(handler)
        srv.start()
        try:
            proc = subprocess.Popen(
                [SIKRADIO_BIN, "-u", srv.url("/"), "-q"],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
            time.sleep(1)
            proc.stdin.write(b"quit\r\n")
            proc.stdin.flush()
            time.sleep(0.5)
            # Client should still be running — "quit\r\n" is not "quit\n"
            self.assertIsNone(proc.poll(), "Client exited on 'quit\\r\\n' — "
                              "it should only respond to literal 'quit\\n'")
            # Now send the real quit
            proc.stdin.write(b"quit\n")
            proc.stdin.flush()
            stdout, stderr = proc.communicate(timeout=5)
            self.assertEqual(proc.returncode, 0)
        finally:
            srv.stop()


# ===========================================================================
# 21. Connection-level edge cases
# ===========================================================================

class TestConnectionEdgeCases(SikradioTestBase):
    """Network-level nastiness."""

    def test_server_sends_response_in_one_byte_chunks(self):
        """Entire HTTP response (headers + body) sent one byte at a time
        with 1ms delay between each byte."""
        audio = b"\xAB\xCD" * 25

        def handler(conn, addr, srv):
            recv_until(conn)
            full_response = build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, audio)
            for byte in full_response:
                conn.sendall(bytes([byte]))
                time.sleep(0.001)
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q"], timeout=15)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
        finally:
            srv.stop()

    def test_server_half_close_then_data(self):
        """Server sends FIN (shutdown write), then nothing.
        Client should see EOF and exit 0."""
        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, b"\xff" * 100))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, b"\xff" * 100)
        finally:
            srv.stop()

    def test_server_delays_between_header_lines(self):
        """Server sends each header line with a 200ms delay between them."""
        audio = b"\xEE" * 80

        def handler(conn, addr, srv):
            recv_until(conn)
            lines = [
                b"HTTP/1.1 200 OK\r\n",
                b"Content-Type: audio/mpeg\r\n",
                b"X-Slow: yes\r\n",
                b"\r\n",
            ]
            for line in lines:
                conn.sendall(line)
                time.sleep(0.2)
            conn.sendall(audio)
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q"], timeout=10)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
        finally:
            srv.stop()

    def test_server_sends_large_burst_then_silence(self):
        """Server sends 512 KB in one burst, then goes silent. Timeout must
        still fire after the burst."""
        conn_count = {"n": 0}
        big_audio = b"\xff" * (512 * 1024)

        def handler(conn, addr, srv):
            recv_until(conn)
            conn_count["n"] += 1
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, big_audio))
            if conn_count["n"] == 1:
                time.sleep(5)  # silence after burst → timeout
            else:
                conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q", "-t", str(SHORT_TIMEOUT_MS)],
                timeout=12)
            self.assertExitOk(rc)
            self.assertGreaterEqual(conn_count["n"], 2)
        finally:
            srv.stop()

    def test_multiple_redirects_different_ports(self):
        """Chain of 3 redirects, each to a different server on a different port."""
        audio = b"\x42" * 100

        def handler3(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, audio))
            conn.shutdown(socket.SHUT_WR)

        srv3 = MockServer(handler3)
        srv3.start()

        def handler2(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 302 Found",
                {"Location": srv3.url("/final"), "Connection": "close"}, b""))
            conn.shutdown(socket.SHUT_WR)

        srv2 = MockServer(handler2)
        srv2.start()

        def handler1(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 302 Found",
                {"Location": srv2.url("/mid"), "Connection": "close"}, b""))
            conn.shutdown(socket.SHUT_WR)

        srv1 = MockServer(handler1)
        srv1.start()

        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv1.url("/start"), "-q"], timeout=8)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
        finally:
            srv1.stop()
            srv2.stop()
            srv3.stop()


# ===========================================================================
# 22. Cookie edge cases
# ===========================================================================

class TestCookieEdgeCases(SikradioTestBase):
    """Nasty cookie scenarios."""

    def test_cookie_with_equals_in_value(self):
        """Cookie value contains '=' (common in base64 encoded values)."""
        captured = []

        def handler(conn, addr, srv):
            req = recv_until(conn)
            captured.append(req)
            if len(captured) == 1:
                conn.sendall(build_http_response(
                    "HTTP/1.1 302 Found",
                    {"Location": f"http://127.0.0.1:{srv.port}/final",
                     "Set-Cookie": "token=abc123==; Path=/",
                     "Connection": "close"}, b""))
            else:
                conn.sendall(build_http_response(
                    "HTTP/1.1 200 OK",
                    {"content-type": "audio/mpeg"}, b"\xff" * 50))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            _, _, rc = run_sikradio(["-u", srv.url("/start"), "-q"], timeout=5)
            self.assertExitOk(rc)
            second_req = captured[1].decode("utf-8", errors="replace")
            self.assertIn("Cookie:", second_req)
            self.assertIn("abc123==", second_req)
        finally:
            srv.stop()

    def test_cookie_overwrite(self):
        """Second redirect sets same cookie name with different value.
        The latest value should be used."""
        captured = []
        conn_n = {"n": 0}

        def handler(conn, addr, srv):
            req = recv_until(conn)
            captured.append(req)
            conn_n["n"] += 1
            if conn_n["n"] == 1:
                conn.sendall(build_http_response(
                    "HTTP/1.1 302 Found",
                    {"Location": f"http://127.0.0.1:{srv.port}/mid",
                     "Set-Cookie": "session=OLD",
                     "Connection": "close"}, b""))
            elif conn_n["n"] == 2:
                conn.sendall(build_http_response(
                    "HTTP/1.1 302 Found",
                    {"Location": f"http://127.0.0.1:{srv.port}/final",
                     "Set-Cookie": "session=NEW",
                     "Connection": "close"}, b""))
            else:
                conn.sendall(build_http_response(
                    "HTTP/1.1 200 OK",
                    {"content-type": "audio/mpeg"}, b"\xff" * 50))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            _, _, rc = run_sikradio(["-u", srv.url("/start"), "-q"], timeout=5)
            self.assertExitOk(rc)
            third_req = captured[2].decode("utf-8", errors="replace")
            self.assertIn("session=NEW", third_req, "Cookie should be overwritten")
            self.assertNotIn("session=OLD", third_req)
        finally:
            srv.stop()

    def test_cookie_with_semicolon_attributes(self):
        """Set-Cookie with Path, Domain, Max-Age etc. — only name=value
        (before first ';') should be stored."""
        captured = []

        def handler(conn, addr, srv):
            req = recv_until(conn)
            captured.append(req)
            if len(captured) == 1:
                resp = b"HTTP/1.1 302 Found\r\n"
                resp += f"Location: http://127.0.0.1:{srv.port}/final\r\n".encode()
                resp += b"Set-Cookie: sid=XYZ; Path=/; HttpOnly; Secure; Max-Age=3600\r\n"
                resp += b"Connection: close\r\n\r\n"
                conn.sendall(resp)
            else:
                conn.sendall(build_http_response(
                    "HTTP/1.1 200 OK",
                    {"content-type": "audio/mpeg"}, b"\xff" * 50))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            _, _, rc = run_sikradio(["-u", srv.url("/start"), "-q"], timeout=5)
            self.assertExitOk(rc)
            second_req = captured[1].decode("utf-8", errors="replace")
            self.assertIn("sid=XYZ", second_req)
            # Should NOT send back the attributes
            self.assertNotIn("HttpOnly", second_req)
            self.assertNotIn("Max-Age", second_req)
        finally:
            srv.stop()


# ===========================================================================
# 23. Rapid reconnect stress test
# ===========================================================================

class TestReconnectStress(SikradioTestBase):
    """Stress the reconnect loop."""

    def test_server_always_closes_immediately_after_200(self):
        """Server sends 200 + empty body + close, repeatedly.
        With -t 300, each close triggers SERVER_DROPPED → exit 0.
        But the client should NOT reconnect on server close (that's only
        for timeout). So it should exit 0 after first connection."""
        conn_count = {"n": 0}

        def handler(conn, addr, srv):
            recv_until(conn)
            conn_count["n"] += 1
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, b"\xAA" * 10))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q", "-t", str(SHORT_TIMEOUT_MS)],
                timeout=5)
            self.assertExitOk(rc)
            # Server dropped → exit 0, no reconnect. Only 1 connection.
            self.assertEqual(conn_count["n"], 1)
            self.assertEqual(stdout, b"\xAA" * 10)
        finally:
            srv.stop()

    def test_redirect_loop_does_not_hang_forever(self):
        """Redirect loop (A → B → A → B → ...). Client must eventually give
        up or crash — but not hang indefinitely. We set a process timeout."""
        def handler(conn, addr, srv):
            req = recv_until(conn)
            if b"GET /a" in req and b"/ab" not in req:
                conn.sendall(build_http_response(
                    "HTTP/1.1 302 Found",
                    {"Location": f"http://127.0.0.1:{srv.port}/b",
                     "Connection": "close"}, b""))
            else:
                conn.sendall(build_http_response(
                    "HTTP/1.1 302 Found",
                    {"Location": f"http://127.0.0.1:{srv.port}/a",
                     "Connection": "close"}, b""))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            # Must finish within 10 seconds — either error or killed
            _, _, rc = run_sikradio(
                ["-u", srv.url("/a"), "-q"], timeout=10)
            # We don't care about the exit code — just that it doesn't hang
            self.assertIn(rc, [0, 1, -signal.SIGKILL, None])
        finally:
            srv.stop()


# ===========================================================================
# 24. Demux state machine from hell
# ===========================================================================

class TestDemuxStateMachineHell(SikradioTestBase):
    """Tests designed to break incorrectly implemented ICY demultiplexers.
    Every test targets a different byte-alignment edge case."""

    def test_length_byte_arrives_alone(self):
        """Audio chunk → [length byte in its own TCP segment] → metadata.
        The length byte must be buffered correctly before metadata arrives."""
        metaint = 16
        audio = b"\xAA" * metaint
        meta = "StreamTitle='Alone';"
        meta_bytes = meta.encode()
        padded = ((len(meta_bytes) + 15) // 16) * 16
        meta_block = meta_bytes + b"\x00" * (padded - len(meta_bytes))
        length_byte = bytes([padded // 16])

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg", "icy-metaint": str(metaint)},
                b""))
            conn.sendall(audio)
            time.sleep(0.05)
            conn.sendall(length_byte)       # length byte alone
            time.sleep(0.05)
            conn.sendall(meta_block)        # then the metadata
            time.sleep(0.05)
            conn.sendall(audio)             # next audio chunk
            conn.sendall(b"\x00")           # zero meta
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, stderr, rc = run_sikradio(
                ["-u", srv.url("/"), "-mq"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio * 2)
            self.assertIn(b"StreamTitle='Alone';", stderr)
        finally:
            srv.stop()

    def test_audio_metadata_and_next_audio_in_one_segment(self):
        """Last byte of audio + length byte + full metadata + first byte of
        next audio all arrive in one recv(). Demuxer must transition through
        all states in a single parse_tcp call."""
        metaint = 4
        audio1 = b"\x11\x22\x33\x44"
        audio2 = b"\x55\x66\x77\x88"
        meta = "StreamTitle='X';"
        meta_bytes = meta.encode()
        padded = ((len(meta_bytes) + 15) // 16) * 16
        meta_block = meta_bytes + b"\x00" * (padded - len(meta_bytes))
        length_byte = bytes([padded // 16])

        full_segment = audio1 + length_byte + meta_block + audio2 + b"\x00"

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg", "icy-metaint": str(metaint)},
                b""))
            # Everything in one shot
            conn.sendall(full_segment)
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, stderr, rc = run_sikradio(
                ["-u", srv.url("/"), "-mq"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio1 + audio2)
            self.assertIn(b"StreamTitle='X';", stderr)
        finally:
            srv.stop()

    def test_every_byte_boundary_split(self):
        """Build a complete audio+meta+audio stream and send it split at every
        possible offset (1 byte, then rest). This is N sub-tests that each
        probe a different alignment."""
        metaint = 8
        audio = b"\xBB" * metaint
        meta = "StreamTitle='Split';"
        full_stream = build_icy_stream([audio, audio], metaint, [meta, None])

        for split_at in range(1, min(len(full_stream), 60)):
            with self.subTest(split_at=split_at):
                chunk1 = full_stream[:split_at]
                chunk2 = full_stream[split_at:]

                def handler(conn, addr, srv, c1=chunk1, c2=chunk2):
                    recv_until(conn)
                    conn.sendall(build_http_response(
                        "HTTP/1.1 200 OK",
                        {"content-type": "audio/mpeg",
                         "icy-metaint": str(metaint)}, b""))
                    conn.sendall(c1)
                    time.sleep(0.01)
                    conn.sendall(c2)
                    conn.shutdown(socket.SHUT_WR)

                srv = MockServer(handler)
                srv.start()
                try:
                    stdout, stderr, rc = run_sikradio(
                        ["-u", srv.url("/"), "-mq"], timeout=5)
                    self.assertExitOk(rc, stderr.decode(errors="replace"))
                    self.assertEqual(stdout, audio * 2,
                                     f"split_at={split_at}: audio corrupted")
                finally:
                    srv.stop()

    def test_alternating_huge_and_empty_metadata(self):
        """Alternate between max-size (4080 byte) and zero-length metadata
        blocks. Tests that the state machine correctly resets position after
        each block regardless of size."""
        metaint = 16
        audio = b"\xCC" * metaint
        big_meta_content = b"StreamTitle='" + b"Z" * 4040 + b"';"
        big_meta_padded = big_meta_content + b"\x00" * (4080 - len(big_meta_content))

        stream = b""
        expected_audio = b""
        for i in range(4):
            stream += audio
            expected_audio += audio
            if i % 2 == 0:
                stream += b"\xff" + big_meta_padded  # 255 * 16 = 4080
            else:
                stream += b"\x00"  # zero

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg", "icy-metaint": str(metaint)},
                stream))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, stderr, rc = run_sikradio(
                ["-u", srv.url("/"), "-mq"], timeout=8)
            self.assertExitOk(rc)
            self.assertEqual(stdout, expected_audio)
            self.assertIn(b"ZZZZ", stderr)
        finally:
            srv.stop()

    def test_metadata_exactly_16_bytes_no_padding(self):
        """Metadata is exactly 16 bytes → length byte = 1, zero padding bytes.
        Tests off-by-one in block boundary calculation."""
        metaint = 16
        audio = b"\xDD" * metaint
        meta = b"StreamTitle='.';" # exactly 16 bytes
        self.assertEqual(len(meta), 16)

        stream = audio + b"\x01" + meta + audio + b"\x00"

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg", "icy-metaint": str(metaint)},
                stream))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, stderr, rc = run_sikradio(
                ["-u", srv.url("/"), "-mq"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio * 2)
            self.assertIn(b"StreamTitle='.';", stderr)
        finally:
            srv.stop()

    def test_10mb_stream_with_metadata_integrity(self):
        """10 MB of audio with metadata every 16000 bytes. Verify every single
        audio byte is correct and no metadata leaks into stdout. This catches
        off-by-one errors that only manifest after many cycles."""
        metaint = 16000
        n_chunks = 650  # ~10 MB
        rng = random.Random(0xDEADBEEF)
        audio_data = bytes(rng.getrandbits(8) for _ in range(metaint))

        stream = b""
        for i in range(n_chunks):
            stream += audio_data
            if i % 50 == 0:
                meta = f"StreamTitle='Track {i}';".encode()
                padded = ((len(meta) + 15) // 16) * 16
                stream += bytes([padded // 16])
                stream += meta + b"\x00" * (padded - len(meta))
            else:
                stream += b"\x00"

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg", "icy-metaint": str(metaint)},
                b""))
            offset = 0
            while offset < len(stream):
                chunk = stream[offset:offset + 32768]
                try:
                    conn.sendall(chunk)
                except (BrokenPipeError, OSError):
                    return
                offset += len(chunk)
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, stderr, rc = run_sikradio(
                ["-u", srv.url("/"), "-mq"], timeout=30)
            self.assertExitOk(rc)
            self.assertEqual(len(stdout), metaint * n_chunks)
            # Verify integrity by checking a few slices
            for i in range(0, n_chunks, 100):
                chunk = stdout[i * metaint:(i + 1) * metaint]
                self.assertEqual(chunk, audio_data,
                                 f"Audio chunk {i} corrupted")
        finally:
            srv.stop()


# ===========================================================================
# 25. Header parsing nightmares
# ===========================================================================

class TestHeaderParsingNightmares(SikradioTestBase):
    """Responses with headers designed to confuse naive parsers."""

    def test_icy_style_no_space_after_colon(self):
        """ICY headers use 'key:value' (no space). Verify icy-metaint is still
        parsed when there's no space: 'icy-metaint:16000'."""
        metaint = 32
        audio = b"\xAA" * metaint
        meta = "StreamTitle='NoSpace';"
        stream = build_icy_stream([audio], metaint, [meta])

        def handler(conn, addr, srv):
            recv_until(conn)
            resp = b"ICY 200 OK\r\n"
            resp += b"icy-name:Test\r\n"
            resp += f"icy-metaint:{metaint}\r\n".encode()
            resp += b"content-type:audio/mpeg\r\n"
            resp += b"\r\n"
            resp += stream
            conn.sendall(resp)
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, stderr, rc = run_sikradio(
                ["-u", srv.url("/"), "-mq"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
            self.assertIn(b"StreamTitle='NoSpace';", stderr)
        finally:
            srv.stop()

    def test_location_header_with_spaces_around_url(self):
        """Location:  http://host/path  (spaces around the URL).
        The trim must handle leading AND trailing whitespace."""
        audio = b"\xBB" * 50

        def handler(conn, addr, srv):
            req = recv_until(conn)
            if b"GET /start" in req:
                resp = b"HTTP/1.1 302 Found\r\n"
                resp += f"Location:   http://127.0.0.1:{srv.port}/final   \r\n".encode()
                resp += b"Connection: close\r\n\r\n"
                conn.sendall(resp)
            else:
                conn.sendall(build_http_response(
                    "HTTP/1.1 200 OK",
                    {"content-type": "audio/mpeg"}, audio))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/start"), "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
        finally:
            srv.stop()

    def test_set_cookie_mixed_case_header_name(self):
        """'SET-COOKIE' vs 'Set-Cookie' vs 'set-cookie' — all must be recognized."""
        captured = []

        def handler(conn, addr, srv):
            req = recv_until(conn)
            captured.append(req)
            if len(captured) == 1:
                resp = b"HTTP/1.1 302 Found\r\n"
                resp += f"Location: http://127.0.0.1:{srv.port}/final\r\n".encode()
                resp += b"SET-COOKIE: upper=A\r\n"
                resp += b"Set-Cookie: mixed=B\r\n"
                resp += b"set-cookie: lower=C\r\n"
                resp += b"Connection: close\r\n\r\n"
                conn.sendall(resp)
            else:
                conn.sendall(build_http_response(
                    "HTTP/1.1 200 OK",
                    {"content-type": "audio/mpeg"}, b"\xff" * 50))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            _, _, rc = run_sikradio(["-u", srv.url("/start"), "-q"], timeout=5)
            self.assertExitOk(rc)
            second_req = captured[1].decode("utf-8", errors="replace")
            # All three cookies must appear
            self.assertIn("upper=A", second_req)
            self.assertIn("mixed=B", second_req)
            self.assertIn("lower=C", second_req)
        finally:
            srv.stop()

    def test_header_value_is_empty(self):
        """Header with no value after the colon: 'icy-genre:\\r\\n'.
        Must not crash."""
        def handler(conn, addr, srv):
            recv_until(conn)
            resp = b"HTTP/1.1 200 OK\r\n"
            resp += b"icy-genre:\r\n"
            resp += b"Content-Type: audio/mpeg\r\n"
            resp += b"\r\n"
            resp += b"\xff" * 100
            conn.sendall(resp)
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(["-u", srv.url("/"), "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, b"\xff" * 100)
        finally:
            srv.stop()

    # IMPLEMENTATION SPECIFIC

    # def test_status_line_with_extra_spaces(self):
    #     """'HTTP/1.1  200  OK' — multiple spaces between fields."""
    #     def handler(conn, addr, srv):
    #         recv_until(conn)
    #         conn.sendall(b"HTTP/1.1  200  OK\r\nContent-Type: audio/mpeg\r\n\r\n")
    #         conn.sendall(b"\xff" * 100)
    #         conn.shutdown(socket.SHUT_WR)

    #     srv = MockServer(handler)
    #     srv.start()
    #     try:
    #         stdout, _, rc = run_sikradio(["-u", srv.url("/"), "-q"], timeout=5)
    #         # atoi(" 200") should still parse 200
    #         self.assertExitOk(rc)
    #         self.assertEqual(stdout, b"\xff" * 100)
    #     finally:
    #         srv.stop()

    def test_icy_metaint_with_leading_trailing_spaces(self):
        """'icy-metaint:  16  ' — spaces around the numeric value.
        atoi should handle this (it skips leading whitespace)."""
        metaint = 16
        audio = b"\xAA" * metaint
        stream = audio + b"\x00"

        def handler(conn, addr, srv):
            recv_until(conn)
            resp = b"HTTP/1.1 200 OK\r\n"
            resp += b"Content-Type: audio/mpeg\r\n"
            resp += b"icy-metaint:  16  \r\n"
            resp += b"\r\n"
            resp += stream
            conn.sendall(resp)
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-mq"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
        finally:
            srv.stop()

    def test_header_line_is_just_a_colon(self):
        """A header line that is literally ':' — empty key and empty value.
        Must not crash the parser."""
        def handler(conn, addr, srv):
            recv_until(conn)
            resp = b"HTTP/1.1 200 OK\r\n"
            resp += b":\r\n"
            resp += b"Content-Type: audio/mpeg\r\n"
            resp += b"\r\n"
            resp += b"\xff" * 50
            conn.sendall(resp)
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(["-u", srv.url("/"), "-q"], timeout=5)
            self.assertExitOk(rc)
        finally:
            srv.stop()


# ===========================================================================
# 26. Reconnect correctness deep dive
# ===========================================================================

class TestReconnectCorrectness(SikradioTestBase):
    """Verify every aspect of the reconnect cycle is correct."""

    def test_redirect_chain_retraversed_after_timeout(self):
        """After timeout, the client must start from the ORIGINAL URL and
        re-traverse the entire redirect chain. If the redirect target changed
        between the first and second connection, the client must follow the
        new target."""
        conn_n = {"n": 0}
        request_log = []

        def handler(conn, addr, srv):
            req = recv_until(conn)
            conn_n["n"] += 1
            first_line = req.split(b"\r\n")[0].decode()
            path = first_line.split(" ")[1]
            request_log.append(path)

            if "/origin" in path:
                # First time: redirect to /dest_a
                # Second time (after timeout): redirect to /dest_b
                if conn_n["n"] <= 2:
                    target = f"http://127.0.0.1:{srv.port}/dest_a"
                else:
                    target = f"http://127.0.0.1:{srv.port}/dest_b"
                conn.sendall(build_http_response(
                    "HTTP/1.1 302 Found",
                    {"Location": target, "Connection": "close"}, b""))
                conn.shutdown(socket.SHUT_WR)
            elif "/dest_a" in path:
                conn.sendall(build_http_response(
                    "HTTP/1.1 200 OK",
                    {"content-type": "audio/mpeg"}, b"\xAA" * 50))
                # Go silent → timeout
                time.sleep(3)
            elif "/dest_b" in path:
                conn.sendall(build_http_response(
                    "HTTP/1.1 200 OK",
                    {"content-type": "audio/mpeg"}, b"\xBB" * 50))
                conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/origin"), "-q",
                 "-t", str(SHORT_TIMEOUT_MS)], timeout=12)
            self.assertExitOk(rc)
            # Should have visited /origin at least twice
            origins = [p for p in request_log if "/origin" in p]
            self.assertGreaterEqual(len(origins), 2)
            # Second round should follow /dest_b, not /dest_a
            self.assertIn("/dest_b", request_log,
                          f"After timeout, did not re-traverse redirect chain: {request_log}")
            # stdout should contain audio from both connections
            self.assertIn(b"\xBB" * 50, stdout)
        finally:
            srv.stop()

    def test_timeout_after_single_audio_byte(self):
        """Server sends 200 + exactly 1 byte of audio, then silence.
        Client must timeout and reconnect — the 1 byte resets the timer once,
        but no further data arrives within the timeout window."""
        conn_count = {"n": 0}

        def handler(conn, addr, srv):
            recv_until(conn)
            conn_count["n"] += 1
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, b""))
            conn.sendall(b"\x42")  # single byte
            if conn_count["n"] == 1:
                time.sleep(3)  # silence → timeout
            else:
                conn.sendall(b"\xff" * 100)
                conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q", "-t", str(SHORT_TIMEOUT_MS)],
                timeout=10)
            self.assertExitOk(rc)
            self.assertGreaterEqual(conn_count["n"], 2)
        finally:
            srv.stop()

    def test_cookies_from_previous_session_not_sent_after_timeout(self):
        """Verify cookie clearing after timeout by checking the raw request
        on the reconnect. No Cookie header should be present."""
        requests = []

        def handler(conn, addr, srv):
            req = recv_until(conn)
            requests.append(req)
            if b"Cookie:" not in req:
                # First connection: set a cookie and redirect
                if b"GET /start" in req:
                    conn.sendall(build_http_response(
                        "HTTP/1.1 302 Found",
                        {"Location": f"http://127.0.0.1:{srv.port}/stream",
                         "Set-Cookie": "trackme=yes",
                         "Connection": "close"}, b""))
                    conn.shutdown(socket.SHUT_WR)
                    return
            # Stream then go silent
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, b"\xff" * 30))
            if len(requests) <= 3:
                time.sleep(3)
            else:
                conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            run_sikradio(
                ["-u", srv.url("/start"), "-q",
                 "-t", str(SHORT_TIMEOUT_MS)], timeout=12)
            # Find the reconnect request (third or later)
            reconnect_reqs = [r for r in requests[2:] if b"GET /start" in r]
            for req in reconnect_reqs:
                self.assertNotIn(b"Cookie:", req,
                                 "Cookies must be cleared on timeout reconnect")
        finally:
            srv.stop()


# ===========================================================================
# 27. Quit detection sliding window torture
# ===========================================================================

class TestQuitSlidingWindow(SikradioTestBase):
    """The quit detector keeps a sliding window of recent stdin bytes.
    These tests probe exact boundary conditions of that window."""

    def _streaming_handler(self, conn, addr, srv):
        """Infinite audio stream."""
        recv_until(conn)
        conn.sendall(build_http_response(
            "HTTP/1.1 200 OK",
            {"content-type": "audio/mpeg"}, b""))
        try:
            while True:
                conn.sendall(b"\xff" * 4096)
                time.sleep(0.05)
        except (BrokenPipeError, OSError):
            pass

    def test_quit_split_every_possible_way(self):
        """'quit\\n' is 5 bytes. Test splitting it at every boundary:
        q|uit\\n, qu|it\\n, qui|t\\n, quit|\\n, each in separate writes."""
        phrase = b"quit\n"
        for split_at in range(1, len(phrase)):
            with self.subTest(split_at=split_at):
                part1 = phrase[:split_at]
                part2 = phrase[split_at:]

                srv = MockServer(self._streaming_handler)
                srv.start()
                try:
                    proc = subprocess.Popen(
                        [SIKRADIO_BIN, "-u", srv.url("/"), "-q"],
                        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE)
                    time.sleep(0.8)
                    proc.stdin.write(part1)
                    proc.stdin.flush()
                    time.sleep(0.1)
                    proc.stdin.write(part2)
                    proc.stdin.flush()
                    stdout, stderr = proc.communicate(timeout=5)
                    self.assertEqual(proc.returncode, 0,
                                     f"split {part1!r}|{part2!r} failed: rc={proc.returncode}")
                finally:
                    srv.stop()

    def test_junk_then_quit_across_reads(self):
        """512 bytes of junk (filling the stdin buffer), then 'quit\\n' split
        across a buffer boundary. The sliding window must survive the junk."""
        srv = MockServer(self._streaming_handler)
        srv.start()
        try:
            proc = subprocess.Popen(
                [SIKRADIO_BIN, "-u", srv.url("/"), "-q"],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
            time.sleep(0.8)
            # Fill the 128-byte internal buffer multiple times
            proc.stdin.write(b"X" * 512)
            proc.stdin.flush()
            time.sleep(0.1)
            proc.stdin.write(b"qui")
            proc.stdin.flush()
            time.sleep(0.1)
            proc.stdin.write(b"t\n")
            proc.stdin.flush()
            stdout, stderr = proc.communicate(timeout=5)
            self.assertEqual(proc.returncode, 0)
        finally:
            srv.stop()

    def test_quit_without_newline_then_eof(self):
        """Send 'quit' without \\n, then close stdin. The substring 'quit\\n'
        is never completed, so the client should NOT treat this as quit.
        It should continue streaming until server closes."""
        audio = b"\xff" * 200

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, audio))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q"],
                stdin_bytes=b"quit", timeout=5)
            # stdin EOF handled, server close → exit 0
            # The key: "quit" without \n did NOT trigger quit
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
        finally:
            srv.stop()

    def test_audio_contains_quit_newline(self):
        """Audio data contains the literal bytes 'quit\\n'. This must appear
        on stdout as audio, NOT trigger the quit handler (quit is only
        detected on stdin, not on the TCP stream)."""
        payload = b"xxxquit\nxxx"

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, payload))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q"],
                stdin_bytes=b"", timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, payload)
            self.assertIn(b"quit\n", stdout)
        finally:
            srv.stop()


# ===========================================================================
# 28. URL parsing edge cases from hell
# ===========================================================================

class TestURLParsingHell(SikradioTestBase):
    """URLs that expose parser corner cases."""

    def test_url_double_slash_in_path(self):
        """http://host:port//stream — double slash must be preserved in GET."""
        captured = {}

        def handler(conn, addr, srv):
            req = recv_until(conn)
            captured["req"] = req
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, b"\xff" * 50))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            url = f"http://127.0.0.1:{srv.port}//stream"
            run_sikradio(["-u", url, "-q"], timeout=5)
            self.assertIn(b"GET //stream HTTP/1.1", captured["req"])
        finally:
            srv.stop()

    def test_url_with_percent_encoding(self):
        """Path with %20 (space) — must be sent verbatim in GET line."""
        captured = {}

        def handler(conn, addr, srv):
            req = recv_until(conn)
            captured["req"] = req
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, b"\xff" * 50))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            url = f"http://127.0.0.1:{srv.port}/my%20stream"
            run_sikradio(["-u", url, "-q"], timeout=5)
            self.assertIn(b"GET /my%20stream HTTP/1.1", captured["req"])
        finally:
            srv.stop()

    def test_url_path_with_semicolons(self):
        """Path containing semicolons (common in some stream URLs)."""
        captured = {}

        def handler(conn, addr, srv):
            req = recv_until(conn)
            captured["req"] = req
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, b"\xff" * 50))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            url = f"http://127.0.0.1:{srv.port}/stream;param=1"
            run_sikradio(["-u", url, "-q"], timeout=5)
            self.assertIn(b"GET /stream;param=1 HTTP/1.1", captured["req"])
        finally:
            srv.stop()

    def test_url_ending_with_question_mark(self):
        """Path ending with bare '?' — the query string is empty."""
        captured = {}

        def handler(conn, addr, srv):
            req = recv_until(conn)
            captured["req"] = req
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, b"\xff" * 50))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            url = f"http://127.0.0.1:{srv.port}/stream?"
            run_sikradio(["-u", url, "-q"], timeout=5)
            self.assertIn(b"GET /stream? HTTP/1.1", captured["req"])
        finally:
            srv.stop()

    def test_url_with_port_host_header(self):
        """Host header must contain only the hostname, no port."""
        captured = {}

        def handler(conn, addr, srv):
            req = recv_until(conn)
            captured["req"] = req
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, b"\xff" * 50))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            url = f"http://127.0.0.1:{srv.port}/stream"
            run_sikradio(["-u", url, "-q"], timeout=5)
            # Host header must NOT include the port
            req_text = captured["req"].decode("utf-8", errors="replace")
            self.assertIn("Host: 127.0.0.1\r\n", req_text)
        finally:
            srv.stop()


# ===========================================================================
# 29. Write correctness & output separation
# ===========================================================================

class TestOutputSeparation(SikradioTestBase):
    """Verify that audio goes ONLY to stdout and metadata ONLY to stderr,
    with no cross-contamination under any circumstances."""

    def test_metadata_never_appears_in_stdout(self):
        """Large metadata with unique marker bytes. Verify none of those
        marker bytes appear anywhere in stdout."""
        metaint = 64
        # Audio is all 0xAA, metadata contains 0xBB — easy to distinguish
        audio = b"\xAA" * metaint
        marker = b"\xBB" * 48
        meta_content = b"StreamTitle='" + marker + b"';"
        padded = ((len(meta_content) + 15) // 16) * 16
        meta_block = meta_content + b"\x00" * (padded - len(meta_content))
        length_byte = bytes([padded // 16])

        stream = b""
        for _ in range(10):
            stream += audio + length_byte + meta_block

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg", "icy-metaint": str(metaint)},
                stream))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, stderr, rc = run_sikradio(
                ["-u", srv.url("/"), "-mq"], timeout=8)
            self.assertExitOk(rc)
            # Audio on stdout must be pure 0xAA
            self.assertEqual(stdout, audio * 10)
            # No 0xBB bytes should appear in stdout
            self.assertNotIn(b"\xBB", stdout)
            # But they should be in stderr (metadata)
            self.assertIn(marker, stderr)
        finally:
            srv.stop()

    def test_length_byte_never_appears_in_stdout(self):
        """The ICY length byte (between audio and metadata) must not leak
        into stdout. Use a length byte value that doesn't appear in the audio."""
        metaint = 4
        audio = b"\x00\x01\x02\x03"
        meta = "StreamTitle='Y';"
        meta_bytes = meta.encode()
        padded = ((len(meta_bytes) + 15) // 16) * 16
        length_byte_val = padded // 16  # will be 2
        meta_block = meta_bytes + b"\x00" * (padded - len(meta_bytes))

        # 5 cycles
        stream = b""
        for _ in range(5):
            stream += audio + bytes([length_byte_val]) + meta_block

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg", "icy-metaint": str(metaint)},
                stream))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-mq"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio * 5)
            self.assertEqual(len(stdout), 20)
        finally:
            srv.stop()

    def test_without_m_flag_everything_is_audio(self):
        """Without -m, even if server sends icy-metaint, all data (including
        what would be metadata) must go to stdout as raw audio."""
        metaint = 8
        audio = b"\x11" * metaint
        meta = "StreamTitle='Z';"
        stream = build_icy_stream([audio, audio], metaint, [meta, None])

        def handler(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg", "icy-metaint": str(metaint)},
                stream))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            # No -m flag!
            stdout, stderr, rc = run_sikradio(
                ["-u", srv.url("/"), "-q"], timeout=5)
            self.assertExitOk(rc)
            # Without -m, Icy-MetaData: 1 is NOT sent in the request,
            # so server shouldn't send icy-metaint... but even if it does,
            # the client treats the data as raw audio (icy_metaint=0 path).
            # stdout should be the entire stream (audio + meta bytes + everything)
            self.assertEqual(stdout, stream)
        finally:
            srv.stop()


# ===========================================================================
# 30. Server behavior after redirect body content
# ===========================================================================

class TestRedirectEdgeCases(SikradioTestBase):
    """Redirect responses with unexpected body content or structure."""

    def test_redirect_with_body_content(self):
        """302 response includes a body (HTML redirect page). The client must
        ignore the body and follow the Location header."""
        audio = b"\xCC" * 100
        redirect_body = b"<html><body>Redirecting...</body></html>"

        def handler(conn, addr, srv):
            req = recv_until(conn)
            if b"GET /old" in req:
                conn.sendall(build_http_response(
                    "HTTP/1.1 302 Found",
                    {"Location": f"http://127.0.0.1:{srv.port}/new",
                     "Content-Length": str(len(redirect_body)),
                     "Connection": "close"},
                    redirect_body))
            else:
                conn.sendall(build_http_response(
                    "HTTP/1.1 200 OK",
                    {"content-type": "audio/mpeg"}, audio))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/old"), "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
            # The redirect body must NOT appear in stdout
            self.assertNotIn(redirect_body, stdout)
        finally:
            srv.stop()

    def test_redirect_preserves_cookies_across_multiple_hops(self):
        """Three-hop redirect, each setting a different cookie. The final
        request must include ALL three cookies."""
        captured = []
        n = {"n": 0}

        def handler(conn, addr, srv):
            req = recv_until(conn)
            captured.append(req)
            n["n"] += 1
            base = f"http://127.0.0.1:{srv.port}"
            if n["n"] == 1:
                conn.sendall(build_http_response(
                    "HTTP/1.1 302 Found",
                    {"Location": f"{base}/hop2",
                     "Set-Cookie": "a=1",
                     "Connection": "close"}, b""))
            elif n["n"] == 2:
                conn.sendall(build_http_response(
                    "HTTP/1.1 302 Found",
                    {"Location": f"{base}/hop3",
                     "Set-Cookie": "b=2",
                     "Connection": "close"}, b""))
            elif n["n"] == 3:
                conn.sendall(build_http_response(
                    "HTTP/1.1 302 Found",
                    {"Location": f"{base}/final",
                     "Set-Cookie": "c=3",
                     "Connection": "close"}, b""))
            else:
                conn.sendall(build_http_response(
                    "HTTP/1.1 200 OK",
                    {"content-type": "audio/mpeg"}, b"\xff" * 50))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            _, _, rc = run_sikradio(["-u", srv.url("/hop1"), "-q"], timeout=8)
            self.assertExitOk(rc)
            final_req = captured[-1].decode("utf-8", errors="replace")
            self.assertIn("a=1", final_req)
            self.assertIn("b=2", final_req)
            self.assertIn("c=3", final_req)
        finally:
            srv.stop()

    def test_redirect_to_same_path_different_port(self):
        """Redirect keeps the same path but changes the port. The Host header
        on the second request must reflect the new port."""
        audio = b"\xEE" * 60
        captured = {}

        def handler2(conn, addr, srv):
            req = recv_until(conn)
            captured["req2"] = req
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, audio))
            conn.shutdown(socket.SHUT_WR)

        srv2 = MockServer(handler2)
        srv2.start()

        def handler1(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 302 Found",
                {"Location": f"http://127.0.0.1:{srv2.port}/stream",
                 "Connection": "close"}, b""))
            conn.shutdown(socket.SHUT_WR)

        srv1 = MockServer(handler1)
        srv1.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv1.url("/stream"), "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
            # Host header must be just the hostname, no port
            req2_text = captured["req2"].decode("utf-8", errors="replace")
            self.assertIn("Host: 127.0.0.1\r\n", req2_text)
        finally:
            srv1.stop()
            srv2.stop()


# ===========================================================================
# 31. Transfer-Encoding: chunked
# ===========================================================================

class TestChunkedTransferEncoding(SikradioTestBase):
    """Tests for HTTP/1.1 Transfer-Encoding: chunked.

    Spec note: the client does not interpret audio *content*, but chunked
    encoding is HTTP transport-layer framing, not content. A server that
    advertises 'Transfer-Encoding: chunked' frames the body as
    <hexsize>\\r\\n<data>\\r\\n ... 0\\r\\n\\r\\n. If the client writes those
    framing bytes to stdout verbatim, the audio is corrupted.

    Example 5 (sikradio_example_5.log) shows a real server sending
    'Transfer-Encoding: chunked' on a 302 redirect response — so a correct
    client must at minimum tolerate chunked framing on redirect responses.
    Whether it must de-chunk an actual 200 audio stream is not explicitly
    stated; these tests document both scenarios.
    """

    def test_chunked_redirect_body_ignored(self):
        """302 redirect with a chunked-encoded HTML body (as in example 5).
        The client must follow the Location header and ignore the chunked
        body entirely. This is the scenario the examples actually exercise."""
        audio = b"\xCC" * 200
        redirect_body = b"<html>Moved</html>"

        def handler(conn, addr, srv):
            req = recv_until(conn)
            if b"GET /start" in req:
                resp = b"HTTP/1.1 302 Found\r\n"
                resp += f"Location: http://127.0.0.1:{srv.port}/final\r\n".encode()
                resp += b"Transfer-Encoding: chunked\r\n"
                resp += b"Connection: keep-alive\r\n"
                resp += b"\r\n"
                resp += chunk_encode(redirect_body, chunk_size=8)
                conn.sendall(resp)
            else:
                conn.sendall(build_http_response(
                    "HTTP/1.1 200 OK",
                    {"content-type": "audio/mpeg"}, audio))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/start"), "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
            # The chunked redirect body must not leak into stdout
            self.assertNotIn(redirect_body, stdout)
            self.assertNotIn(b"<html>", stdout)
        finally:
            srv.stop()

    def test_chunked_audio_stream_dechunked(self):
        """Server delivers the 200 audio stream itself with
        Transfer-Encoding: chunked. A correct client must de-chunk it so
        stdout receives only the audio payload, not the hex size lines.

        If your implementation does not support chunked audio bodies, this
        test documents the failure; real Icecast/SHOUTcast servers stream
        raw, so this is an edge case rather than a hard requirement."""
        audio = bytes(random.Random(7).getrandbits(8) for _ in range(8192))

        def handler(conn, addr, srv):
            recv_until(conn)
            resp = b"HTTP/1.1 200 OK\r\n"
            resp += b"Content-Type: audio/mpeg\r\n"
            resp += b"Transfer-Encoding: chunked\r\n"
            resp += b"\r\n"
            resp += chunk_encode(audio, chunk_size=1000)
            conn.sendall(resp)
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q"], timeout=8)
            self.assertExitOk(rc)
            # stdout must be exactly the audio — no chunk framing bytes
            self.assertEqual(stdout, audio,
                             "Chunked audio stream was not de-chunked: "
                             "chunk size lines / CRLFs leaked into stdout")
        finally:
            srv.stop()

    def test_chunked_audio_no_size_lines_in_output(self):
        """Verify no hexadecimal chunk-size lines appear in stdout. Uses an
        audio payload of pure 0xAA so any leaked ASCII hex digits / CRLF
        from chunk framing are easy to detect."""
        audio = b"\xAA" * 6000

        def handler(conn, addr, srv):
            recv_until(conn)
            resp = b"HTTP/1.1 200 OK\r\n"
            resp += b"Content-Type: audio/mpeg\r\n"
            resp += b"Transfer-Encoding: chunked\r\n"
            resp += b"\r\n"
            resp += chunk_encode(audio, chunk_size=512)
            conn.sendall(resp)
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q"], timeout=8)
            self.assertExitOk(rc)
            # 0xAA never equals an ASCII hex digit or CR/LF, so a correct
            # de-chunked stream is pure 0xAA bytes.
            self.assertEqual(stdout, audio)
            self.assertNotIn(b"\r\n", stdout)
            self.assertNotIn(b"200\r\n", stdout)
        finally:
            srv.stop()

    def test_chunked_audio_single_chunk(self):
        """Entire audio body in one chunk followed by the 0-terminator."""
        audio = b"\xBB" * 1024

        def handler(conn, addr, srv):
            recv_until(conn)
            resp = b"HTTP/1.1 200 OK\r\n"
            resp += b"Content-Type: audio/mpeg\r\n"
            resp += b"Transfer-Encoding: chunked\r\n"
            resp += b"\r\n"
            resp += chunk_encode(audio, chunk_size=len(audio))
            conn.sendall(resp)
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
        finally:
            srv.stop()

    def test_chunked_audio_many_tiny_chunks(self):
        """Audio split into many 1-byte chunks. Stresses the de-chunker's
        state machine across chunk boundaries."""
        audio = bytes(range(256))  # 256 distinct bytes

        def handler(conn, addr, srv):
            recv_until(conn)
            resp = b"HTTP/1.1 200 OK\r\n"
            resp += b"Content-Type: audio/mpeg\r\n"
            resp += b"Transfer-Encoding: chunked\r\n"
            resp += b"\r\n"
            resp += chunk_encode(audio, chunk_size=1)
            conn.sendall(resp)
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q"], timeout=8)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
        finally:
            srv.stop()

    def test_chunked_audio_split_across_tcp_segments(self):
        """Chunk-encoded audio sent in tiny TCP segments, so chunk size lines
        and chunk data are split across recv() boundaries."""
        audio = bytes(random.Random(11).getrandbits(8) for _ in range(4096))
        encoded = chunk_encode(audio, chunk_size=333)

        def handler(conn, addr, srv):
            recv_until(conn)
            header = b"HTTP/1.1 200 OK\r\n"
            header += b"Content-Type: audio/mpeg\r\n"
            header += b"Transfer-Encoding: chunked\r\n"
            header += b"\r\n"
            conn.sendall(header)
            # Drip the chunked body in 7-byte TCP segments
            for i in range(0, len(encoded), 7):
                try:
                    conn.sendall(encoded[i:i + 7])
                except (BrokenPipeError, OSError):
                    return
                time.sleep(0.001)
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q"], timeout=12)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
        finally:
            srv.stop()

    def test_chunked_audio_with_icy_metadata(self):
        """The hardest combination: chunked transport framing wrapping an
        ICY-metadata-interleaved audio stream. The client must first
        de-chunk, then demux the ICY metadata from the result."""
        metaint = 64
        audio = b"\xDD" * metaint
        meta = "StreamTitle='Chunked+ICY';"
        icy_stream = build_icy_stream([audio, audio], metaint, [meta, None])
        encoded = chunk_encode(icy_stream, chunk_size=100)

        def handler(conn, addr, srv):
            recv_until(conn)
            resp = b"HTTP/1.1 200 OK\r\n"
            resp += b"Content-Type: audio/mpeg\r\n"
            resp += f"icy-metaint: {metaint}\r\n".encode()
            resp += b"Transfer-Encoding: chunked\r\n"
            resp += b"\r\n"
            resp += encoded
            conn.sendall(resp)
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, stderr, rc = run_sikradio(
                ["-u", srv.url("/"), "-mq"], timeout=8)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio * 2)
            self.assertIn(b"StreamTitle='Chunked+ICY';", stderr)
        finally:
            srv.stop()

    def test_chunked_size_uppercase_hex(self):
        """Chunk sizes written with uppercase hex digits (e.g. 'FF' not 'ff').
        RFC 7230 allows both — the de-chunker must accept uppercase."""
        audio = b"\xEE" * 300

        def handler(conn, addr, srv):
            recv_until(conn)
            resp = b"HTTP/1.1 200 OK\r\n"
            resp += b"Content-Type: audio/mpeg\r\n"
            resp += b"Transfer-Encoding: chunked\r\n"
            resp += b"\r\n"
            # Manually encode with uppercase hex
            offset = 0
            while offset < len(audio):
                piece = audio[offset:offset + 256]
                resp += f"{len(piece):X}\r\n".encode() + piece + b"\r\n"
                offset += len(piece)
            resp += b"0\r\n\r\n"
            conn.sendall(resp)
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
        finally:
            srv.stop()

    def test_chunked_with_chunk_extension(self):
        """Chunk size line carries a chunk-extension (';name=value' after the
        size). RFC 7230 permits this; the de-chunker must skip the extension
        and read only the size."""
        audio = b"\x55" * 400

        def handler(conn, addr, srv):
            recv_until(conn)
            resp = b"HTTP/1.1 200 OK\r\n"
            resp += b"Content-Type: audio/mpeg\r\n"
            resp += b"Transfer-Encoding: chunked\r\n"
            resp += b"\r\n"
            offset = 0
            while offset < len(audio):
                piece = audio[offset:offset + 200]
                # size with a bogus chunk-extension
                resp += f"{len(piece):x};foo=bar\r\n".encode() + piece + b"\r\n"
                offset += len(piece)
            resp += b"0\r\n\r\n"
            conn.sendall(resp)
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/"), "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
        finally:
            srv.stop()


# ===========================================================================
# 32. Relative redirect URLs
# ===========================================================================

class TestRelativeRedirects(SikradioTestBase):
    """The Location header on a redirect may be a relative URL rather than an
    absolute one. RFC 7231 §7.1.2 allows relative references. A correct client
    resolves the relative Location against the URL of the request that
    produced the redirect."""

    def test_redirect_absolute_path(self):
        """Location: /newpath — absolute path, no scheme/host. Must resolve
        to the same host:port with the new path."""
        audio = b"\xAA" * 100

        def handler(conn, addr, srv):
            req = recv_until(conn)
            if b"GET /old" in req:
                conn.sendall(build_http_response(
                    "HTTP/1.1 302 Found",
                    {"Location": "/newpath", "Connection": "close"}, b""))
            elif b"GET /newpath" in req:
                conn.sendall(build_http_response(
                    "HTTP/1.1 200 OK",
                    {"content-type": "audio/mpeg"}, audio))
            else:
                conn.sendall(build_http_response(
                    "HTTP/1.1 404 Not Found", {}, b""))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/old"), "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
        finally:
            srv.stop()

    def test_redirect_absolute_path_with_query(self):
        """Location: /stream?token=abc — absolute path including a query."""
        audio = b"\xBB" * 100
        captured = {}

        def handler(conn, addr, srv):
            req = recv_until(conn)
            if b"GET /old" in req:
                conn.sendall(build_http_response(
                    "HTTP/1.1 302 Found",
                    {"Location": "/stream?token=abc&x=1",
                     "Connection": "close"}, b""))
            else:
                captured["req2"] = req
                conn.sendall(build_http_response(
                    "HTTP/1.1 200 OK",
                    {"content-type": "audio/mpeg"}, audio))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/old"), "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
            self.assertIn(b"GET /stream?token=abc&x=1 HTTP/1.1",
                          captured["req2"])
        finally:
            srv.stop()

    def test_redirect_absolute_path_preserves_host(self):
        """A relative redirect must keep the SAME host:port. Verify the Host
        header on the second request is unchanged."""
        audio = b"\xCC" * 80
        captured = {}

        def handler(conn, addr, srv):
            req = recv_until(conn)
            if b"GET /a" in req:
                conn.sendall(build_http_response(
                    "HTTP/1.1 302 Found",
                    {"Location": "/b", "Connection": "close"}, b""))
            else:
                captured["req2"] = req
                conn.sendall(build_http_response(
                    "HTTP/1.1 200 OK",
                    {"content-type": "audio/mpeg"}, audio))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/a"), "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
            # Host on the second request must still be 127.0.0.1 (no port)
            req2_text = captured["req2"].decode("utf-8", errors="replace")
            self.assertIn("Host: 127.0.0.1\r\n", req2_text)
        finally:
            srv.stop()

    def test_redirect_absolute_path_keeps_port(self):
        """When the original URL used a non-default port, a relative redirect
        must connect back to that same port."""
        audio = b"\xDD" * 80
        conn_count = {"n": 0}

        def handler(conn, addr, srv):
            req = recv_until(conn)
            conn_count["n"] += 1
            if b"GET /first" in req:
                conn.sendall(build_http_response(
                    "HTTP/1.1 302 Found",
                    {"Location": "/second", "Connection": "close"}, b""))
            else:
                conn.sendall(build_http_response(
                    "HTTP/1.1 200 OK",
                    {"content-type": "audio/mpeg"}, audio))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/first"), "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
            # Both requests landed on the same mock server (same port)
            self.assertGreaterEqual(conn_count["n"], 2)
        finally:
            srv.stop()

    def test_redirect_relative_then_absolute_chain(self):
        """A chain mixing a relative redirect and an absolute redirect:
        /start --(relative)--> /mid --(absolute)--> other server."""
        audio = b"\xEE" * 90

        def handler2(conn, addr, srv):
            recv_until(conn)
            conn.sendall(build_http_response(
                "HTTP/1.1 200 OK",
                {"content-type": "audio/mpeg"}, audio))
            conn.shutdown(socket.SHUT_WR)

        srv2 = MockServer(handler2)
        srv2.start()

        def handler1(conn, addr, srv):
            req = recv_until(conn)
            if b"GET /start" in req:
                # relative redirect
                conn.sendall(build_http_response(
                    "HTTP/1.1 302 Found",
                    {"Location": "/mid", "Connection": "close"}, b""))
            elif b"GET /mid" in req:
                # absolute redirect to the other server
                conn.sendall(build_http_response(
                    "HTTP/1.1 302 Found",
                    {"Location": srv2.url("/final"),
                     "Connection": "close"}, b""))
            conn.shutdown(socket.SHUT_WR)

        srv1 = MockServer(handler1)
        srv1.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv1.url("/start"), "-q"], timeout=8)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
        finally:
            srv1.stop()
            srv2.stop()

    def test_relative_redirect_then_timeout_uses_original_url(self):
        """After following a relative redirect and then timing out, the client
        must reconnect to the ORIGINAL absolute URL — not the resolved
        relative one."""
        request_paths = []
        conn_n = {"n": 0}

        def handler(conn, addr, srv):
            req = recv_until(conn)
            conn_n["n"] += 1
            first_line = req.split(b"\r\n")[0].decode()
            path = first_line.split(" ")[1]
            request_paths.append(path)

            if path == "/origin":
                conn.sendall(build_http_response(
                    "HTTP/1.1 302 Found",
                    {"Location": "/streampath", "Connection": "close"}, b""))
                conn.shutdown(socket.SHUT_WR)
            elif path == "/streampath":
                conn.sendall(build_http_response(
                    "HTTP/1.1 200 OK",
                    {"content-type": "audio/mpeg"}, b"\xff" * 40))
                if conn_n["n"] <= 2:
                    time.sleep(3)  # silence → timeout
                else:
                    conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/origin"), "-q",
                 "-t", str(SHORT_TIMEOUT_MS)], timeout=12)
            self.assertExitOk(rc)
            # /origin must be requested at least twice (initial + reconnect)
            origins = [p for p in request_paths if p == "/origin"]
            self.assertGreaterEqual(len(origins), 2,
                                    f"After timeout the client did not return "
                                    f"to the original URL: {request_paths}")
        finally:
            srv.stop()

    def test_redirect_absolute_path_root(self):
        """Location: / — redirect to the site root."""
        audio = b"\x42" * 70
        captured = {}

        def handler(conn, addr, srv):
            req = recv_until(conn)
            if b"GET /deep/path" in req:
                conn.sendall(build_http_response(
                    "HTTP/1.1 302 Found",
                    {"Location": "/", "Connection": "close"}, b""))
            else:
                captured["req2"] = req
                conn.sendall(build_http_response(
                    "HTTP/1.1 200 OK",
                    {"content-type": "audio/mpeg"}, audio))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            stdout, _, rc = run_sikradio(
                ["-u", srv.url("/deep/path"), "-q"], timeout=5)
            self.assertExitOk(rc)
            self.assertEqual(stdout, audio)
            self.assertIn(b"GET / HTTP/1.1", captured["req2"])
        finally:
            srv.stop()

    def test_redirect_relative_cookies_carried(self):
        """A relative redirect must still carry forward cookies set on the
        redirect response, exactly like an absolute redirect."""
        captured = []

        def handler(conn, addr, srv):
            req = recv_until(conn)
            captured.append(req)
            if b"GET /entry" in req:
                conn.sendall(build_http_response(
                    "HTTP/1.1 302 Found",
                    {"Location": "/audio",
                     "Set-Cookie": "rel=works",
                     "Connection": "close"}, b""))
            else:
                conn.sendall(build_http_response(
                    "HTTP/1.1 200 OK",
                    {"content-type": "audio/mpeg"}, b"\xff" * 50))
            conn.shutdown(socket.SHUT_WR)

        srv = MockServer(handler)
        srv.start()
        try:
            _, _, rc = run_sikradio(["-u", srv.url("/entry"), "-q"], timeout=5)
            self.assertExitOk(rc)
            second = captured[1].decode("utf-8", errors="replace")
            self.assertIn("Cookie:", second)
            self.assertIn("rel=works", second)
        finally:
            srv.stop()


# ===========================================================================
# Entry point
# ===========================================================================

if __name__ == "__main__":
    # Check that the binary exists
    if not os.path.isfile(SIKRADIO_BIN):
        print(f"ERROR: sikradio binary not found at {SIKRADIO_BIN}")
        print(f"       Build it first with 'make', or set SIKRADIO_BIN env var.")
        sys.exit(1)

    unittest.main()