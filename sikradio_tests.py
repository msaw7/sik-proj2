"""
sikradio_tests.py — unit tests for the sikradio internet radio client.

Key design rules
----------------
- NEVER call proc.wait() before draining proc.stdout: pipe buffer deadlock.
  Use proc.communicate() which drains both pipes concurrently.
- For interactive tests (quit, SIGINT) use launch_client_interactive() and
  call proc.communicate(timeout=...) after sending the signal.
- Never use a hardcoded port for "refused" connections: use refused_url().
- On test FAILURE, stdout and stderr are written to <TestName>.log so that
  the grader has a concrete dump of what the client produced.
  On PASS, no log file is left behind.
"""

import os
import signal
import socket
import threading
import time
import unittest
import subprocess
import sys


# ---------------------------------------------------------------------------
# Locate the client binary
# ---------------------------------------------------------------------------

CLIENT_BIN_CANDIDATES = (
    "./sikradio",
    "./cmake-build-release/sikradio",
    "./cmake-build-debug/sikradio",
)
CLIENT_BIN = None

LOG_DIR = "test_logs"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

LOCALHOST = "127.0.0.1"


def terminate_process(proc, timeout=1.0):
    """Send SIGINT and wait; kill if it doesn't respond."""
    if proc.poll() is None:
        proc.send_signal(signal.SIGINT)
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def free_port():
    """Return an ephemeral port number that is free right now."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def refused_url():
    """
    Return a URL whose TCP connection will be refused immediately.
    Binds a real socket, records its port, closes it — so the port is
    guaranteed free but nothing is listening; connect() returns ECONNREFUSED.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((LOCALHOST, 0))
        port = s.getsockname()[1]
    return f"http://{LOCALHOST}:{port}/"


class MockServer:
    """
    Minimal TCP server that calls handler(conn) per accepted connection.
    reuse=True allows multiple sequential connections (for reconnect tests).
    """

    def __init__(self, handler, reuse=False):
        self.handler = handler
        self.reuse   = reuse
        self.port    = free_port()
        self._sock   = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((LOCALHOST, self.port))
        self._sock.listen(8)
        self._sock.settimeout(5.0)
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._stop   = threading.Event()

    def start(self):
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()
        try:
            self._sock.close()
        except OSError:
            pass

    def _serve(self):
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except OSError:
                break
            try:
                self.handler(conn)
            finally:
                try:
                    conn.close()
                except OSError:
                    pass
            if not self.reuse:
                break

    def url(self, path="/stream"):
        return f"http://{LOCALHOST}:{self.port}{path}"


def http_200(conn, body=b"AUDIO", metaint=None):
    headers  = "HTTP/1.1 200 OK\r\n"
    headers += "Content-Type: audio/mpeg\r\n"
    if metaint is not None:
        headers += f"icy-metaint: {metaint}\r\n"
    headers += "Connection: close\r\n\r\n"
    conn.sendall(headers.encode() + body)


def icy_200(conn, body=b"AUDIO"):
    headers  = "ICY 200 OK\r\n"
    headers += "icy-name: Test Radio\r\n"
    headers += "content-type: audio/mpeg\r\n"
    headers += "\r\n"
    conn.sendall(headers.encode() + body)


def http_302(conn, location):
    resp  = "HTTP/1.1 302 Found\r\n"
    resp += f"Location: {location}\r\n"
    resp += "Content-Length: 0\r\nConnection: close\r\n\r\n"
    conn.sendall(resp.encode())


def read_request(conn):
    """Read from conn until the blank line ending HTTP headers."""
    data = b""
    conn.settimeout(3.0)
    while b"\r\n\r\n" not in data and b"\n\n" not in data:
        try:
            chunk = conn.recv(4096)
        except socket.timeout:
            break
        if not chunk:
            break
        data += chunk
    return data.decode(errors="replace")


def build_icy_stream(audio_chunk: bytes, metaint: int, title: str) -> bytes:
    """
    Build one complete ICY cycle (used by metadata tests):
        [metaint audio bytes][1-byte block-count][block-count*16 meta bytes]
    Note: no trailing audio — the caller decides what follows.
    """
    assert len(audio_chunk) == metaint
    raw_meta    = f"StreamTitle='{title}';\x00".encode()
    blocks      = (len(raw_meta) + 15) // 16
    meta_blob   = raw_meta.ljust(blocks * 16, b"\x00")
    length_byte = bytes([blocks])
    return audio_chunk + length_byte + meta_blob


# ---------------------------------------------------------------------------
# Base class — failure logging
# ---------------------------------------------------------------------------

class SikradioTestBase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        global CLIENT_BIN
        for exe in CLIENT_BIN_CANDIDATES:
            if os.path.exists(exe):
                CLIENT_BIN = exe
                return
        raise FileNotFoundError(
            f"sikradio binary not found in {CLIENT_BIN_CANDIDATES}"
        )

    def setUp(self):
        # Track whether any subtest in this method has failed.
        # Prevents a passing subtest from deleting a failing sibling's log.
        self._had_failure = False

    def _log_path(self, label=None):
        os.makedirs(LOG_DIR, exist_ok=True)
        raw = label if label else self.id()
        safe = raw.replace(" ", "_").replace("/", "-").replace(":", "-")
        return os.path.join(LOG_DIR, f"{safe}.log")

    def _write_log(self, rc, stdout, stderr, args, label=None):
        """Write a failure dump to <LOG_DIR>/<label>.log."""
        self._had_failure = True
        path = self._log_path(label)
        with open(path, "wb") as f:
            f.write(f"=== COMMAND ===\n{CLIENT_BIN} {' '.join(args)}\n".encode())
            f.write(f"=== RETURN CODE ===\n{rc}\n".encode())
            f.write(b"=== STDOUT ===\n")
            f.write(stdout[:4096])
            if len(stdout) > 4096:
                f.write(f"\n... ({len(stdout)} bytes total, truncated)\n".encode())
            f.write(b"\n=== STDERR ===\n")
            f.write(stderr)

    def _remove_log(self, label=None):
        # Never remove if any failure has occurred in this test method.
        if self._had_failure:
            return
        path = self._log_path(label)
        if os.path.exists(path):
            os.remove(path)

    def assertTestPasses(self, rc, stdout, stderr, args, assertions, label=None):
        """
        Run assertions(rc, stdout, stderr).
        On failure: write <label>.log and re-raise.
        On success: remove stale log for this label (only if no prior failure).
        Pass a unique label per subTest iteration so each gets its own log file.
        """
        try:
            assertions(rc, stdout, stderr)
        except AssertionError:
            self._write_log(rc, stdout, stderr, args, label)
            raise
        else:
            self._remove_log(label)


    def launch_client(self, args, input_bytes=None, comm_timeout=15):
        """
        Launch and communicate() immediately (drains stdout concurrently).
        Returns (returncode, stdout_bytes, stderr_bytes).
        On timeout: kills, writes log, fails the test.
        """
        cmd  = [CLIENT_BIN] + args
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.addCleanup(terminate_process, proc)
        try:
            out, err = proc.communicate(input=input_bytes, timeout=comm_timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            out, err = proc.communicate()
            self._write_log(-1, out, err, args)
            self.fail(f"client timed out after {comm_timeout}s: {args}")
        return proc.returncode, out, err

    def launch_client_interactive(self, args):
        """
        Launch without waiting — caller drives stdin and terminates.
        The cleanup terminates the process if the test forgets.
        For interactive tests the caller is responsible for logging on failure.
        """
        cmd  = [CLIENT_BIN] + args
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.addCleanup(terminate_process, proc)
        return proc

    def communicate_interactive(self, proc, args, timeout=5):
        """
        Drain an interactive process.  On timeout: kill, write log, fail.
        Returns (stdout_bytes, stderr_bytes).
        """
        try:
            out, err = proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            out, err = proc.communicate()
            self._write_log(proc.returncode, out, err, args)
            self.fail(f"interactive client timed out after {timeout}s")
        return out, err


# ===========================================================================
# 1. Argument Tests
# ===========================================================================

class ArgTests(SikradioTestBase):

    def test_missing_mandatory_url(self):
        args = ["-m", "-t", "5000", "-v", "2"]
        rc, out, err = self.launch_client(args)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: self.assertEqual(1, r))

    def test_invalid_args(self):
        cases = [
            (["-t", "99"],    "t_too_low"),
            (["-t", "100001"],"t_too_high"),
            (["-t", "abc"],   "t_not_a_number"),
            (["-v", "-1"],    "v_negative"),
            (["-v", "5"],     "v_too_high"),
            (["-v", "abc"],   "v_not_a_number"),
            (["-x"],          "unknown_flag"),
        ]
        for extra_args, name in cases:
            with self.subTest(name=name):
                args = ["-u", refused_url()] + extra_args
                rc, out, err = self.launch_client(args)
                self.assertTestPasses(
                    rc, out, err, args,
                    lambda r, o, e: self.assertEqual(1, r),
                    label=f"{self.id()}.{name}",
                )

    def test_valid_minimum_invocation(self):
        """Connection is refused, but not due to bad arguments."""
        args = ["-u", refused_url(), "-t", "100"]
        rc, out, err = self.launch_client(args)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertEqual(1, r),
            self.assertNotEqual(-signal.SIGSEGV, r),
        ))

    def test_both_ip_flags_accepted(self):
        """-4 and -6 together is not an argument error per spec."""
        args = ["-u", refused_url(), "-4", "-6", "-t", "100"]
        rc, out, err = self.launch_client(args)
        self.assertTestPasses(
            rc, out, err, args,
            lambda r, o, e: self.assertNotEqual(-signal.SIGSEGV, r)
        )

    def test_q_flag_produces_no_stderr(self):
        def handler(conn):
            read_request(conn)
            http_200(conn, body=b"X")

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)

        args = ["-u", srv.url(), "-q"]
        rc, out, err = self.launch_client(args)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: self.assertEqual(b"", e))

    def test_params_without_space(self):
        """-t100 (no space between flag and value) accepted by getopt."""
        args = ["-u", refused_url(), "-t100"]
        rc, out, err = self.launch_client(args)
        self.assertTestPasses(
            rc, out, err, args,
            lambda r, o, e: self.assertNotEqual(-signal.SIGSEGV, r)
        )

    def test_blocked_flags(self):
        """-m46 parsed as -m -4 -6."""
        args = ["-u", refused_url(), "-m46", "-t100"]
        rc, out, err = self.launch_client(args)
        self.assertTestPasses(
            rc, out, err, args,
            lambda r, o, e: self.assertNotEqual(-signal.SIGSEGV, r)
        )

    def test_timeout_boundary_100_accepted(self):
        args = ["-u", refused_url(), "-t", "100"]
        rc, out, err = self.launch_client(args)
        # rc=1 from connection failure, not argument error
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: self.assertEqual(1, r))

    def test_timeout_boundary_100000_accepted(self):
        args = ["-u", refused_url(), "-t", "100000"]
        rc, out, err = self.launch_client(args)
        self.assertTestPasses(
            rc, out, err, args,
            lambda r, o, e: self.assertNotEqual(-signal.SIGSEGV, r)
        )


# ===========================================================================
# 2. Quit Tests
# ===========================================================================

class QuitTests(SikradioTestBase):

    def _streaming_server(self, conn):
        """Infinite audio — stays open until peer closes."""
        read_request(conn)
        conn.sendall(
            b"HTTP/1.1 200 OK\r\nContent-Type: audio/mpeg\r\n"
            b"Connection: keep-alive\r\n\r\n"
        )
        conn.settimeout(0.1)
        try:
            while True:
                conn.sendall(b"\xff" * 512)
                time.sleep(0.05)
        except OSError:
            pass

    def test_quit_exits_zero(self):
        srv = MockServer(self._streaming_server, reuse=True).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url(), "-t", "5000"]

        proc = self.launch_client_interactive(args)
        time.sleep(0.5)
        proc.stdin.write(b"quit\n")
        proc.stdin.flush()

        out, err = self.communicate_interactive(proc, args, timeout=4)
        self.assertTestPasses(
            proc.returncode, out, err, args,
            lambda r, o, e: self.assertEqual(0, r)
        )

    def test_non_quit_input_ignored(self):
        """Non-quit input must not terminate the client."""
        srv = MockServer(self._streaming_server, reuse=True).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url(), "-t", "5000"]

        proc = self.launch_client_interactive(args)
        time.sleep(0.4)
        proc.stdin.write(b"hello\n")
        proc.stdin.flush()
        time.sleep(0.4)

        still_running = proc.poll() is None
        terminate_process(proc)
        out, err = b"", b""
        try:
            out, err = proc.communicate(timeout=1)
        except subprocess.TimeoutExpired:
            pass

        self.assertTestPasses(
            proc.returncode or 0, out, err, args,
            lambda r, o, e: self.assertTrue(still_running, "client exited on non-quit input")
        )

    # def test_quit_during_header_read(self):
    #     """
    #     'quit' while blocked waiting for HTTP headers must cause clean exit.
    #     Spec Q5: the client waits for data; 'quit' must interrupt that wait.
    #     """
    #     silent_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #     silent_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    #     port = free_port()
    #     silent_sock.bind((LOCALHOST, port))
    #     silent_sock.listen(1)
    #     self.addCleanup(silent_sock.close)

    #     released = threading.Event()

    #     def accept_and_hold():
    #         try:
    #             conn, _ = silent_sock.accept()
    #             released.wait(timeout=10)
    #             conn.close()
    #         except OSError:
    #             pass

    #     threading.Thread(target=accept_and_hold, daemon=True).start()

    #     args = ["-u", f"http://{LOCALHOST}:{port}/", "-t", "10000"]
    #     proc = self.launch_client_interactive(args)
    #     time.sleep(0.6)
    #     proc.stdin.write(b"quit\n")
    #     proc.stdin.flush()

    #     out, err = self.communicate_interactive(proc, args, timeout=4)
    #     released.set()

    #     self.assertTestPasses(
    #         proc.returncode, out, err, args,
    #         lambda r, o, e: self.assertEqual(0, r)
    #     )


# ===========================================================================
# 3. Server Interaction Tests
# ===========================================================================

class ServerTests(SikradioTestBase):

    def test_audio_forwarded_to_stdout(self):
        payload = b"\x00\x01\x02\x03" * 256

        def handler(conn):
            read_request(conn)
            http_200(conn, body=payload)

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url(), "-t", "2000"]

        rc, out, err = self.launch_client(args)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertEqual(0, r),
            self.assertIn(payload, o),
        ))

    def test_icy_200_response_accepted(self):
        payload = b"X" * 512

        def handler(conn):
            read_request(conn)
            icy_200(conn, body=payload)

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url(), "-t", "2000"]

        rc, out, err = self.launch_client(args)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertEqual(0, r),
            self.assertIn(payload, o),
        ))

    def test_server_close_exits_zero(self):
        """Spec: server closing connection → exit 0."""
        def handler(conn):
            read_request(conn)
            http_200(conn, body=b"data")

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url(), "-t", "2000"]

        rc, out, err = self.launch_client(args)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: self.assertEqual(0, r))

    def test_audio_bytes_not_corrupted(self):
        """All 256 byte values must survive the pipe verbatim."""
        payload = bytes(range(256)) * 16

        def handler(conn):
            read_request(conn)
            http_200(conn, body=payload)

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url(), "-t", "3000"]

        rc, out, err = self.launch_client(args)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertEqual(0, r),
            self.assertEqual(payload, o),
        ))

    def test_large_audio_stream(self):
        """1 MiB stream must arrive without corruption."""
        payload = b"\xAB\xCD" * (512 * 1024)

        def handler(conn):
            read_request(conn)
            http_200(conn, body=payload)

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url(), "-t", "5000"]

        rc, out, err = self.launch_client(args, comm_timeout=30)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertEqual(0, r),
            self.assertEqual(payload, o),
        ))

    def test_non_200_status_exits_one(self):
        def handler(conn):
            read_request(conn)
            conn.sendall(b"HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n")

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url(), "-t", "2000"]

        rc, out, err = self.launch_client(args)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: self.assertEqual(1, r))

    def test_404_exits_one(self):
        def handler(conn):
            read_request(conn)
            conn.sendall(b"HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n")

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url(), "-t", "2000"]

        rc, out, err = self.launch_client(args)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: self.assertEqual(1, r))

    def test_302_redirect_followed(self):
        payload = b"REDIRECTED_AUDIO"

        def final_handler(conn):
            read_request(conn)
            http_200(conn, body=payload)

        final_srv = MockServer(final_handler).start()
        self.addCleanup(final_srv.stop)

        def redirect_handler(conn):
            read_request(conn)
            http_302(conn, location=final_srv.url())

        redir_srv = MockServer(redirect_handler).start()
        self.addCleanup(redir_srv.stop)
        args = ["-u", redir_srv.url(), "-t", "2000"]

        rc, out, err = self.launch_client(args)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertEqual(0, r),
            self.assertIn(payload, o),
        ))

    def test_302_no_location_exits_one(self):
        def handler(conn):
            read_request(conn)
            conn.sendall(
                b"HTTP/1.1 302 Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url(), "-t", "2000"]

        rc, out, err = self.launch_client(args)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: self.assertEqual(1, r))

    def test_timeout_triggers_reconnect(self):
        """
        Spec: timeout → disconnect and reconnect.
        First connection goes silent after headers; second delivers audio.
        """
        call_count = {"n": 0}

        def handler(conn):
            call_count["n"] += 1
            read_request(conn)
            if call_count["n"] == 1:
                conn.sendall(
                    b"HTTP/1.1 200 OK\r\nContent-Type: audio/mpeg\r\n"
                    b"Connection: close\r\n\r\n"
                )
                time.sleep(4)   # longer than -t 500
            else:
                http_200(conn, body=b"AUDIO_AFTER_RECONNECT")

        srv = MockServer(handler, reuse=True).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url(), "-t", "500", "-v4"]

        rc, out, err = self.launch_client(args, comm_timeout=12)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertEqual(0, r),
            self.assertIn(b"AUDIO_AFTER_RECONNECT", o),
            self.assertGreaterEqual(call_count["n"], 2),
        ))

    def test_timeout_message_on_stderr(self):
        """'data receiving timeout' appears at verbosity >= 1."""
        def handler(conn):
            read_request(conn)
            conn.sendall(
                b"HTTP/1.1 200 OK\r\nContent-Type: audio/mpeg\r\n"
                b"Connection: close\r\n\r\n"
            )
            time.sleep(4)

        srv = MockServer(handler, reuse=True).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url(), "-t", "300", "-v1"]

        proc = self.launch_client_interactive(args)
        time.sleep(1.5)
        proc.send_signal(signal.SIGINT)
        out, err = self.communicate_interactive(proc, args, timeout=3)

        self.assertTestPasses(
            proc.returncode, out, err, args,
            lambda r, o, e: self.assertIn(b"data receiving timeout", e)
        )

    def test_icy_metadata_printed_to_stderr(self):
        """With -m, StreamTitle appears on stderr."""
        metaint = 64
        title   = "Test Artist - Test Song"
        stream  = build_icy_stream(b"A" * metaint, metaint, title)

        def handler(conn):
            read_request(conn)
            headers = (
                f"HTTP/1.1 200 OK\r\nContent-Type: audio/mpeg\r\n"
                f"icy-metaint: {metaint}\r\nConnection: close\r\n\r\n"
            )
            conn.sendall(headers.encode() + stream)

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url(), "-m", "-t", "2000"]

        rc, out, err = self.launch_client(args)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertEqual(0, r),
            self.assertIn(title.encode(), e),
        ))

    def test_metadata_not_in_audio_stream(self):
        """ICY metadata bytes must be stripped from stdout."""
        metaint = 64
        title   = "SHOULD_NOT_BE_IN_AUDIO"
        audio   = b"A" * metaint
        stream  = build_icy_stream(audio, metaint, title)

        def handler(conn):
            read_request(conn)
            headers = (
                f"HTTP/1.1 200 OK\r\nContent-Type: audio/mpeg\r\n"
                f"icy-metaint: {metaint}\r\nConnection: close\r\n\r\n"
            )
            conn.sendall(headers.encode() + stream)

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url(), "-m", "-t", "2000"]

        rc, out, err = self.launch_client(args)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertEqual(0, r),
            self.assertNotIn(title.encode(), o),
            self.assertIn(audio, o),
        ))

    def test_no_icy_header_without_m_flag(self):
        received = {}

        def handler(conn):
            received["req"] = read_request(conn)
            http_200(conn, body=b"X")

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url(), "-t", "2000"]

        rc, out, err = self.launch_client(args)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertNotIn("Icy-MetaData", received.get("req", "")),
        ))

    def test_icy_header_present_with_m_flag(self):
        received = {}

        def handler(conn):
            received["req"] = read_request(conn)
            http_200(conn, body=b"X")

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url(), "-m", "-t", "2000"]

        rc, out, err = self.launch_client(args)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertIn("Icy-MetaData: 1", received.get("req", "")),
        ))

    def test_host_header_correct(self):
        received = {}

        def handler(conn):
            received["req"] = read_request(conn)
            http_200(conn, body=b"X")

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url("/audio"), "-t", "2000"]

        rc, out, err = self.launch_client(args)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertIn(f"Host: {LOCALHOST}:{srv.port}", received.get("req", "")),
        ))

    def test_get_path_correct(self):
        received = {}

        def handler(conn):
            received["req"] = read_request(conn)
            http_200(conn, body=b"X")

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url("/test/path?q=1"), "-t", "2000"]

        rc, out, err = self.launch_client(args)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertIn("GET /test/path?q=1 HTTP/1.1", received.get("req", "")),
        ))

    def test_root_path_when_url_has_no_path(self):
        received = {}

        def handler(conn):
            received["req"] = read_request(conn)
            http_200(conn, body=b"X")

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)
        args = ["-u", f"http://{LOCALHOST}:{srv.port}", "-t", "2000"]

        rc, out, err = self.launch_client(args)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertRegex(received.get("req", ""), r"GET / HTTP/1\.1"),
        ))

    def test_verbosity_0_no_stderr(self):
        def handler(conn):
            read_request(conn)
            http_200(conn, body=b"X")

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url(), "-v0"]

        rc, out, err = self.launch_client(args)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertEqual(0, r),
            self.assertEqual(b"", e),
        ))

    def test_verbosity_1_shows_resolving(self):
        def handler(conn):
            read_request(conn)
            http_200(conn, body=b"X")

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url(), "-v1"]

        rc, out, err = self.launch_client(args)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertEqual(0, r),
            self.assertIn(b"resolving name", e),
        ))

    def test_verbosity_2_shows_error_on_failure(self):
        """At default verbosity, critical errors appear on stderr."""
        args = ["-u", refused_url(), "-t", "200"]
        rc, out, err = self.launch_client(args)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertEqual(1, r),
            self.assertGreater(len(e), 0),
        ))


# ===========================================================================
# 4. Signal Tests
# ===========================================================================

class SignalTests(SikradioTestBase):

    def _infinite_server(self, conn):
        read_request(conn)
        conn.sendall(
            b"HTTP/1.1 200 OK\r\nContent-Type: audio/mpeg\r\n"
            b"Connection: keep-alive\r\n\r\n"
        )
        conn.settimeout(0.1)
        try:
            while True:
                conn.sendall(b"\x00" * 256)
                time.sleep(0.1)
        except OSError:
            pass

    # def test_sigint_exits_zero(self):
    #     srv = MockServer(self._infinite_server, reuse=True).start()
    #     self.addCleanup(srv.stop)
    #     args = ["-u", srv.url(), "-t", "5000"]

    #     proc = self.launch_client_interactive(args)
    #     time.sleep(0.5)
    #     proc.send_signal(signal.SIGINT)
    #     out, err = self.communicate_interactive(proc, args, timeout=3)

    #     self.assertTestPasses(
    #         proc.returncode, out, err, args,
    #         lambda r, o, e: self.assertEqual(0, r)
    #     )

    # def test_sigint_flushes_audio_received_before_signal(self):
    #     """Spec: 'klient wypisuje wszystkie dotychczas odebrane dane'."""
    #     chunk = b"\xFF\xFB\x90\x00" * 256

    #     chunk_srv_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #     chunk_srv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    #     cport = free_port()
    #     chunk_srv_sock.bind((LOCALHOST, cport))
    #     chunk_srv_sock.listen(1)
    #     self.addCleanup(chunk_srv_sock.close)

    #     def serve_chunk():
    #         chunk_srv_sock.settimeout(5)
    #         try:
    #             conn, _ = chunk_srv_sock.accept()
    #             read_request(conn)
    #             conn.sendall(
    #                 b"HTTP/1.1 200 OK\r\nContent-Type: audio/mpeg\r\n"
    #                 b"Connection: keep-alive\r\n\r\n"
    #             )
    #             conn.sendall(chunk)
    #             time.sleep(15)
    #             conn.close()
    #         except OSError:
    #             pass

    #     threading.Thread(target=serve_chunk, daemon=True).start()

    #     args = ["-u", f"http://{LOCALHOST}:{cport}/", "-t", "5000"]
    #     proc = self.launch_client_interactive(args)
    #     time.sleep(1.0)
    #     proc.send_signal(signal.SIGINT)
    #     out, err = self.communicate_interactive(proc, args, timeout=3)

    #     self.assertTestPasses(
    #         proc.returncode, out, err, args, lambda r, o, e: (
    #             self.assertEqual(0, r),
    #             self.assertIn(chunk, o),
    #         )
    #     )


# ===========================================================================
# 5. Malicious & Edge Case Tests
# ===========================================================================

class MaliciousEdgeCaseTests(SikradioTestBase):
    """
    Testy odporności na złośliwe lub błędne zachowanie serwera.

    Nazewnictwo oczekiwanego rc:
      rc=0  server closed connection = STREAM_DONE = clean exit
      rc=1  parse/protocol error or connection failure = EXIT_FAILURE
    """

    # -------------------------------------------------------------------
    # Garbled / truncated status line
    # -------------------------------------------------------------------

    def test_immediate_connection_close(self):
        """Serwer akceptuje i natychmiast zamyka (zero bajtów -> EOF).
        read_line wraca z len=0 -> http_read_response -2 -> rc=1."""
        def handler(conn):
            pass   # MockServer closes after handler returns

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url(), "-t", "2000"]
        rc, out, err = self.launch_client(args)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: self.assertEqual(1, r))

    def test_malformed_status_line(self):
        """Odpowiedź nie zaczyna się od HTTP/ ani ICY -> rc=1."""
        def handler(conn):
            read_request(conn)
            conn.sendall(b"HELLO THIS IS DOG NOT HTTP\r\n\r\n")

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url(), "-t", "2000"]
        rc, out, err = self.launch_client(args)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: self.assertEqual(1, r))

    def test_drop_mid_status_line(self):
        """EOF w połowie linii statusu przed CRLF.
        read_line zwraca 'HTTP/1.1 20', sscanf parsuje 20 != 200 -> rc=1."""
        def handler(conn):
            read_request(conn)
            conn.sendall(b"HTTP/1.1 20")   # brak CRLF, EOF po tym

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url(), "-t", "2000"]
        rc, out, err = self.launch_client(args)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: self.assertEqual(1, r))

    def test_garbled_responses_no_crash(self):
        """
        Kilka wariantów śmieciowych odpowiedzi z serwera.
        Wspólna właściwość: klient nie może zawiesić się ani sypnąć SIGSEGV.
        Każdy wariant ma swój plik logu dzięki unikalnemu label.

        Analiza oczekiwanego rc:
          null_bytes_only     -> nie HTTP/ICY -> rc=1
          eof_mid_protocol    -> 'HTT' nie pasuje -> rc=1
          http_missing_code   -> sscanf nie znajduje liczby -> rc=1 lub rc=0 (impl)
          icy_garbage_code    -> sscanf %d nie czyta 'abc' -> rc=1
          binary_noise        -> nie HTTP/ICY -> rc=1
        """
        cases = [
            (b"\x00\x00\x00\x00",              "null_bytes_only",    1),
            (b"HTT",                            "eof_mid_protocol",   1),
            (b"HTTP/1.1 \r\n\r\n",             "http_missing_code",  1),
            (b"ICY abc OK\r\n\r\n",            "icy_garbage_code",   1),
            (b"\xff\xfe\x00\x01" * 32,         "binary_noise",       1),
        ]
        for payload, name, expected_rc in cases:
            with self.subTest(variation=name):
                # Each iteration needs its own MockServer because
                # MockServer is single-use by default.
                def make_handler(p):
                    def handler(conn):
                        read_request(conn)
                        conn.sendall(p)
                    return handler

                srv = MockServer(make_handler(payload)).start()
                args = ["-u", srv.url(), "-t", "2000"]
                rc, out, err = self.launch_client(args)
                srv.stop()
                self.assertTestPasses(
                    rc, out, err, args,
                    lambda r, o, e, exp=expected_rc: (
                        self.assertNotEqual(-signal.SIGSEGV, r),
                        self.assertEqual(exp, r),
                    ),
                    label=f"{self.id()}.{name}",
                )


    # Bez sensu, to nie jest poprawny komunikat HTTP.

    # def test_status_200_no_crlf_at_all(self):
    #     """
    #     Serwer wysyła kompletną linię statusu ALE bez żadnego CRLF, potem EOF.
    #     read_line dostaje EOF po odczytaniu wszystkich znaków, zwraca linię.
    #     Status 200 parsowany poprawnie, brak nagłówków -> stream_audio ->
    #     conn_read -> EOF (0) -> STREAM_DONE -> rc=0.
    #     Klient nie może się zawiesić na braku \r\n.
    #     """
    #     def handler(conn):
    #         read_request(conn)
    #         conn.sendall(b"HTTP/1.1 200 OK")   # brak \r\n, potem EOF

    #     srv = MockServer(handler).start()
    #     self.addCleanup(srv.stop)
    #     args = ["-u", srv.url(), "-t", "2000"]
    #     rc, out, err = self.launch_client(args)
    #     self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
    #         self.assertNotEqual(-signal.SIGSEGV, r),
    #         self.assertEqual(0, r),   # 200 parsed OK, EOF on audio = clean exit
    #     ))

    # def test_bare_lf_headers(self):
    #     """Serwer używa tylko LF (bez CR) jako separatora linii.
    #     read_line akceptuje sam LF (pomija CR) -> nagłówki parsowane poprawnie -> rc=0."""
    #     payload = b"AUDIO_DATA"

    #     def handler(conn):
    #         read_request(conn)
    #         conn.sendall(
    #             b"HTTP/1.1 200 OK\n"
    #             b"Content-Type: audio/mpeg\n"
    #             b"Connection: close\n"
    #             b"\n"
    #             + payload
    #         )

    #     srv = MockServer(handler).start()
    #     self.addCleanup(srv.stop)
    #     args = ["-u", srv.url(), "-t", "2000"]
    #     rc, out, err = self.launch_client(args)
    #     self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
    #         self.assertEqual(0, r),
    #         self.assertIn(payload, o),
    #     ))

    # -------------------------------------------------------------------
    # Oversized / overflowing headers
    # -------------------------------------------------------------------

    def test_massive_header_value_no_crash(self):
        """Pojedynczy nagłówek z wartością 8 KiB (2x bufor read_line).
        Klient może obciąć wartość lub odrzucić nagłówek, ale nie segfault."""
        def handler(conn):
            read_request(conn)
            conn.sendall(
                b"HTTP/1.1 200 OK\r\n"
                b"X-Oversized: " + (b"A" * 8192) + b"\r\n"
                                                   b"Content-Type: audio/mpeg\r\nConnection: close\r\n\r\n"
                                                   b"AUDIO"
            )

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url(), "-t", "2000"]
        rc, out, err = self.launch_client(args)
        self.assertTestPasses(
            rc, out, err, args,
            lambda r, o, e: self.assertNotEqual(-signal.SIGSEGV, r),
        )

    def test_header_name_no_colon_no_crash(self):
        """Nagłówek bez dwukropka (malformed). Klient powinien go zignorować gracefully."""
        def handler(conn):
            read_request(conn)
            conn.sendall(
                b"HTTP/1.1 200 OK\r\n"
                b"ThisHasNoColonAtAll\r\n"
                b"Content-Type: audio/mpeg\r\nConnection: close\r\n\r\n"
                b"AUDIO"
            )

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url(), "-t", "2000"]
        rc, out, err = self.launch_client(args)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertNotEqual(-signal.SIGSEGV, r),
            self.assertEqual(0, r),
        ))

    def test_hundreds_of_headers_no_crash(self):
        """200 nagłówków (> MAX_HEADERS=64). Klient odrzuca nadmiar, nie segfault."""
        def handler(conn):
            read_request(conn)
            headers = b"HTTP/1.1 200 OK\r\n"
            for i in range(200):
                headers += f"X-Header-{i}: value\r\n".encode()
            headers += b"Content-Type: audio/mpeg\r\nConnection: close\r\n\r\nAUDIO"
            conn.sendall(headers)

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url(), "-t", "2000"]
        rc, out, err = self.launch_client(args)
        self.assertTestPasses(
            rc, out, err, args,
            lambda r, o, e: self.assertNotEqual(-signal.SIGSEGV, r),
        )

    # -------------------------------------------------------------------
    # Mid-stream drops
    # -------------------------------------------------------------------

    def test_drop_mid_audio(self):
        """EOF w połowie audio po prawidłowych nagłówkach.
        Spec: serwer zamknął połączenie -> exit 0. Dane przed dropem są na stdout."""
        payload = b"FIRST_HALF_DATA"

        def handler(conn):
            read_request(conn)
            conn.sendall(
                b"HTTP/1.1 200 OK\r\nContent-Type: audio/mpeg\r\n"
                b"Connection: close\r\n\r\n"
            )
            conn.sendall(payload)

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url(), "-t", "2000"]
        rc, out, err = self.launch_client(args)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertEqual(0, r),
            self.assertIn(payload, o),
        ))

    # To nie jest poprawny komunikat HTTP.

    # def test_drop_mid_headers(self):
    #     """EOF w połowie bloku nagłówków (po statusie, przed pustą linią).
    #     Klient parsuje status 200, kończy nagłówki przy EOF -> audio EOF -> rc=0."""
    #     def handler(conn):
    #         read_request(conn)
    #         # Wysyłamy status i jeden niekompletny nagłówek, potem EOF
    #         conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Typ")

    #     srv = MockServer(handler).start()
    #     self.addCleanup(srv.stop)
    #     args = ["-u", srv.url(), "-t", "2000"]
    #     rc, out, err = self.launch_client(args)
    #     # Status 200 był poprawny; brakujący nagłówek jest pominięty,
    #     # pusta audio faza dostaje EOF natychmiast -> STREAM_DONE -> rc=0
    #     self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
    #         self.assertNotEqual(-signal.SIGSEGV, r),
    #         self.assertEqual(0, r),
    #     ))

    # Bez sensu ? Czemu to miałoby to kończyć się statusem 1 ? Gdzie to jest w treści ?

    # def test_drop_mid_metadata(self):
    #     """EOF w połowie bloku ICY metadanych (po bajcie długości, przed końcem bloku).
    #     icy_read_meta_block dostaje got=0 przed uzupełnieniem bloku -> rc=1."""
    #     def handler(conn):
    #         read_request(conn)
    #         conn.sendall(
    #             b"HTTP/1.1 200 OK\r\nContent-Type: audio/mpeg\r\n"
    #             b"icy-metaint: 5\r\nConnection: close\r\n\r\n"
    #         )
    #         conn.sendall(b"12345")          # pełne metaint=5 bajtów audio
    #         conn.sendall(b"\x05")           # 5 * 16 = 80 bajtów potrzebnych
    #         conn.sendall(b"StreamTitle='X';")  # 16 z 80 -> EOF

    #     srv = MockServer(handler).start()
    #     self.addCleanup(srv.stop)
    #     args = ["-u", srv.url(), "-m", "-t", "2000"]
    #     rc, out, err = self.launch_client(args)
    #     self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
    #         self.assertEqual(1, r),
    #         self.assertNotEqual(-signal.SIGSEGV, r),
    #     ))

    # -------------------------------------------------------------------
    # ICY metadata edge cases
    # -------------------------------------------------------------------

    def test_zero_length_metadata_block(self):
        """Bajt długości = 0x00 (pusty blok, najczęstszy przypadek w praktyce).
        Klient musi go pominąć i kontynuować odczyt audio bez przerwy."""
        metaint = 8

        def handler(conn):
            read_request(conn)
            conn.sendall((
                             "HTTP/1.1 200 OK\r\nContent-Type: audio/mpeg\r\n"
                             f"icy-metaint: {metaint}\r\nConnection: close\r\n\r\n"
                         ).encode())
            conn.sendall(b"AUDIO123")    # 8 bajtów audio
            conn.sendall(b"\x00")        # pusty blok (0 * 16 = 0 bajtów)
            conn.sendall(b"AUDIOBCD")   # kolejne 8 bajtów audio

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url(), "-m", "-t", "2000"]
        rc, out, err = self.launch_client(args)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertEqual(0, r),
            self.assertIn(b"AUDIO123", o),
            self.assertIn(b"AUDIOBCD", o),
            self.assertNotIn(b"\x00", o),   # bajt długości nie może wyciec do stdout
        ))

    def test_huge_metadata_block_no_crash(self):
        """Maksymalny blok ICY (255 * 16 = 4080 bajtów) parsowany bez crashu.
        Po pełnym bloku następuje EOF -> STREAM_DONE -> rc=0."""
        meta = b"StreamTitle='Giant Meta Block';\x00".ljust(4080, b"\x00")

        def handler(conn):
            read_request(conn)
            conn.sendall(
                b"HTTP/1.1 200 OK\r\nContent-Type: audio/mpeg\r\n"
                b"icy-metaint: 5\r\nConnection: close\r\n\r\n"
            )
            conn.sendall(b"12345")  # 5 bajtów audio (= metaint)
            conn.sendall(b"\xFF")   # blok 255 * 16 = 4080 bajtów
            conn.sendall(meta)      # pełny blok -> EOF -> STREAM_DONE

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url(), "-m", "-t", "3000"]
        rc, out, err = self.launch_client(args, comm_timeout=10)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertEqual(0, r),
            self.assertIn(b"Giant Meta Block", e),
            self.assertNotIn(b"\xFF" + meta[:8], o),  # meta nie wycieka do stdout
        ))

    def test_metadata_with_special_chars(self):
        """Tytuł z apostrofem, cudzysłowem i znakami UTF-8 w metadanych."""
        metaint = 16
        title   = "Édith Piaf - L'Hymne à l'amour"
        stream  = build_icy_stream(b"X" * metaint, metaint, title)

        def handler(conn):
            read_request(conn)
            conn.sendall((
                             "HTTP/1.1 200 OK\r\nContent-Type: audio/mpeg\r\n"
                             f"icy-metaint: {metaint}\r\nConnection: close\r\n\r\n"
                         ).encode() + stream)

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url(), "-m", "-t", "2000"]
        rc, out, err = self.launch_client(args)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertNotEqual(-signal.SIGSEGV, r),
            # Tytuł musi pojawić się na stderr (przynajmniej jego fragment ASCII)
            self.assertIn(b"Piaf", e),
        ))

    # -------------------------------------------------------------------
    # Slowloris / drip-feed
    # -------------------------------------------------------------------

    def test_slowloris_drip_audio(self):
        """Dane przychodzą co 200 ms, timeout = 800 ms.
        Każdy recv() wraca przed SO_RCVTIMEO -> klient NIE zrywa połączenia."""
        def handler(conn):
            read_request(conn)
            conn.sendall(
                b"HTTP/1.1 200 OK\r\nContent-Type: audio/mpeg\r\n"
                b"Connection: close\r\n\r\n"
            )
            for _ in range(5):
                conn.sendall(b"A")
                time.sleep(0.2)

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url(), "-t", "800"]
        rc, out, err = self.launch_client(args, comm_timeout=10)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertEqual(0, r),
            self.assertEqual(b"AAAAA", o),
        ))

    def test_drip_feed_exactly_at_timeout_boundary(self):
        """Dane przychodzą co 400 ms, timeout = 300 ms.
        Przerwa PRZEKRACZA timeout -> klient MUSI zerwać i ponowić połączenie."""
        call_count = {"n": 0}

        def handler(conn):
            call_count["n"] += 1
            read_request(conn)
            conn.sendall(
                b"HTTP/1.1 200 OK\r\nContent-Type: audio/mpeg\r\n"
                b"Connection: close\r\n\r\n"
            )
            if call_count["n"] == 1:
                conn.sendall(b"A")
                time.sleep(1.0)   # 1000 ms > 300 ms timeout
            else:
                conn.sendall(b"RECONNECTED")

        srv = MockServer(handler, reuse=True).start()
        self.addCleanup(srv.stop)
        args = ["-u", srv.url(), "-t", "300"]
        rc, out, err = self.launch_client(args, comm_timeout=10)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertEqual(0, r),
            self.assertGreaterEqual(call_count["n"], 2),
        ))

    # -------------------------------------------------------------------
    # Connection failure
    # -------------------------------------------------------------------

    def test_connection_refused_exits_one(self):
        """Spec Q6: brak połączenia = błąd krytyczny uniemożliwiający pracę -> rc=1."""
        args = ["-u", refused_url(), "-t", "100"]
        rc, out, err = self.launch_client(args)
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: self.assertEqual(1, r))

    def test_no_reconnect_on_connection_failure(self):
        """Spec Q6: klient NIE ponawia connect() jeśli się nie udało.
        Mierzymy czas: powinien zakończyć się w < 2 s (nie zapętlać)."""
        args = ["-u", refused_url(), "-t", "5000"]
        start = time.monotonic()
        rc, out, err = self.launch_client(args, comm_timeout=5)
        elapsed = time.monotonic() - start
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertEqual(1, r),
            self.assertLess(elapsed, 2.0,
                            f"client took {elapsed:.2f}s — appears to retry after refused connection"),
        ))


# ===========================================================================
# 6. Timeout Timing Tests
# ===========================================================================

class TimeoutTimingTests(SikradioTestBase):
    """
    Testy weryfikujące precyzję i zachowanie mechanizmu -t timeout.

    Specyfikacja:
      - -t TIMEOUT: limit czasu na odbiór kolejnych danych (ms, zakres 100–100000)
      - Domyślnie 5000 ms
      - Minimalna wymagana rozdzielczość: 500 ms
      - Timeout dotyczy wyłącznie fazy odbierania danych, NIE nawiązywania połączenia

    Metodologia pomiarów:
      Każdy test mierzy czas od momentu gdy serwer przestaje wysyłać dane
      do momentu gdy klient się rozłącza lub wysyła kolejne żądanie.
      Tolerancja: +500 ms ponad nominalne opóźnienie (margines na OS scheduling).
    """

    TIMING_TOLERANCE_MS = 50  # maksymalne dozwolone przekroczenie ponad -t

    def _measure_timeout_firing(self, timeout_ms):
        
        """
        Uruchamia serwer, który wysyła nagłówki 200 OK i milknie.
        Mierzy czas od końca nagłówków do momentu gdy klient się rozłączy
        (co obserwujemy przez recv() zwracające 0 na końcu połączenia).
        Zwraca elapsed_ms.
        """

        
        timing = {}
        disconnect_event = threading.Event()

        def handler(conn):
            if "client_disconnected" in timing:
                return
            read_request(conn)
            conn.sendall(
                b"HTTP/1.1 200 OK\r\nContent-Type: audio/mpeg\r\n"
                b"Connection: close\r\n\r\n"
            )
            timing["headers_sent"] = time.monotonic()
            # Czekamy na rozłączenie klienta (recv zwróci 0)
            conn.settimeout(timeout_ms / 1000.0 * 3)  # 3x timeout jako guard
            try:
                while True:
                    data = conn.recv(1)
                    if not data:
                        break
            except OSError:
                pass
            timing["client_disconnected"] = time.monotonic()
            disconnect_event.set()

        srv = MockServer(handler, reuse=True).start()
        self.addCleanup(srv.stop)

        args = ["-u", srv.url(), "-t", str(timeout_ms)]
        proc = self.launch_client_interactive(args)

        # Czekamy aż klient się rozłączy (timeout powinien zadziałać)
        fired = disconnect_event.wait(timeout=timeout_ms / 1000.0 * 4 + 2.0)
        terminate_process(proc)
        try:
            proc.communicate(timeout=1)
        except subprocess.TimeoutExpired:
            pass

        srv.stop()

        if not fired:
            self.fail(f"Timeout {timeout_ms}ms never fired (server never saw disconnect)")

        elapsed_ms = (timing["client_disconnected"] - timing["headers_sent"]) * 1000
        return elapsed_ms

    def test_timeout_fires_within_tolerance(self):
        """
        Klient musi rozłączyć się po -t 1000 ms (± 500 ms tolerancji).
        Weryfikuje, że timeout w ogóle działa i nie jest zignorowany.
        """
        timeout_ms = 1000
        elapsed = self._measure_timeout_firing(timeout_ms)

        args = ["-u", "mock", "-t", str(timeout_ms)]
        lower = timeout_ms - 100          # nie powinien zadziałać za wcześnie
        upper = timeout_ms + self.TIMING_TOLERANCE_MS

        self.assertTestPasses(0, b"", b"", args, lambda r, o, e: (
            self.assertGreater(elapsed, lower,
                               f"Timeout fired too early: {elapsed:.0f}ms < {lower}ms"),
            self.assertLess(elapsed, upper,
                            f"Timeout fired too late: {elapsed:.0f}ms > {upper}ms"),
        ))

    def test_minimum_timeout_100ms_fires(self):
        """
        Minimalny -t 100 ms musi faktycznie zadziałać (nie być ignorowany).
        """
        timeout_ms = 100
        elapsed = self._measure_timeout_firing(timeout_ms)
        upper = timeout_ms + self.TIMING_TOLERANCE_MS

        args = ["-u", "mock", "-t", str(timeout_ms)]
        self.assertTestPasses(0, b"", b"", args, lambda r, o, e:
        self.assertLess(elapsed, upper,
                        f"Minimum timeout {timeout_ms}ms fired too late: {elapsed:.0f}ms")
                              )

    def test_timeout_500ms_resolution(self):
        """
        Spec: minimalna rozdzielczość timeoutu wynosi 500 ms.
        Przy -t 500 klient musi rozłączyć się nie później niż 1000 ms
        (500 + 500 tolerancji).
        """
        timeout_ms = 500
        elapsed = self._measure_timeout_firing(timeout_ms)
        upper = timeout_ms + self.TIMING_TOLERANCE_MS

        args = ["-u", "mock", "-t", str(timeout_ms)]
        self.assertTestPasses(0, b"", b"", args, lambda r, o, e:
        self.assertLess(elapsed, upper,
                        f"500ms timeout fired too late: {elapsed:.0f}ms > {upper}ms")
                              )

    def test_timeout_not_applied_during_headers(self):
        """
        Spec: timeout dotyczy WYŁĄCZNIE odbierania danych strumieniowych,
        nie fazy nawiązywania połączenia ani czytania nagłówków odpowiedzi.
        Serwer zwleka 800 ms przed wysłaniem nagłówków przy -t 500 ms.
        Klient NIE powinien się rozłączyć przed odebraniem nagłówków.
        """
        # Serwer celowo zwleka 800 ms przed wysłaniem nagłówków.
        # Jeśli timeout działałby od razu po connect(), klient by się rozłączył.
        server_delay_ms = 800
        timeout_ms = 500   # mniejszy niż opóźnienie nagłówków

        payload = b"DATA_AFTER_SLOW_HEADERS"

        def handler(conn):
            read_request(conn)
            time.sleep(server_delay_ms / 1000.0)  # celowe opóźnienie
            http_200(conn, body=payload)

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)

        args = ["-u", srv.url(), "-t", str(timeout_ms)]
        rc, out, err = self.launch_client(args, comm_timeout=10)

        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertEqual(0, r,
                             f"Client timed out during header phase (timeout should not apply there); "
                             f"stderr={e.decode(errors='replace')!r}"),
            self.assertIn(payload, o),
        ))

    def test_data_arriving_before_timeout_resets_it(self):
        """
        Dane przychodzące regularnie co T ms przy -t > T ms NIE powinny
        wyzwalać timeoutu. Sprawdza, że timeout resetuje się po każdym recv().
        """
        interval_ms  = 150   # dane co 150 ms
        timeout_ms   = 600   # timeout co 600 ms — powinien być resetowany
        num_chunks   = 6     # łącznie 6 * 150 ms = 900 ms (1.5× timeout)
        payload_byte = b"D"

        def handler(conn):
            read_request(conn)
            conn.sendall(
                b"HTTP/1.1 200 OK\r\nContent-Type: audio/mpeg\r\n"
                b"Connection: close\r\n\r\n"
            )
            for _ in range(num_chunks):
                conn.sendall(payload_byte)
                time.sleep(interval_ms / 1000.0)

        srv = MockServer(handler).start()
        self.addCleanup(srv.stop)

        args = ["-u", srv.url(), "-t", str(timeout_ms)]
        rc, out, err = self.launch_client(args, comm_timeout=10)

        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertEqual(0, r,
                             "Timeout fired while data was arriving regularly (timeout not being reset)"),
            self.assertEqual(payload_byte * num_chunks, o,
                             f"Expected {num_chunks} bytes, got {len(o)}"),
        ))


# ===========================================================================
# 7. IPv6 Tests
# ===========================================================================

def _ipv6_available():
    """
    Return (available: bool, reason: str).
    Checks that the kernel supports AF_INET6 AND that ::1 is actually usable
    (bind + connect round-trip on loopback).  Both are required for local
    mock-server tests.
    """
    try:
        s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    except OSError as e:
        return False, f"kernel has no AF_INET6 support: {e}"
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
        s.bind(("::1", 0))
        s.listen(1)
        port = s.getsockname()[1]
        c = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        c.settimeout(1.0)
        c.connect(("::1", port))
        c.close()
        s.close()
        return True, "ok"
    except OSError as e:
        return False, f"::1 loopback unusable: {e}"


def _find_ipv6_hostname():
    """
    Return the first hostname that resolves to an IPv6 address on this machine,
    or None.  Tries 'ip6-localhost' then 'localhost' — both appear in the
    standard /etc/hosts on MIMUW student machines as '::1 ip6-localhost localhost'.
    """
    for name in ("ip6-localhost", "localhost"):
        try:
            results = socket.getaddrinfo(
                name, None, socket.AF_INET6, socket.SOCK_STREAM
            )
            if results:
                return name
        except socket.gaierror:
            pass
    return None


# Module-level checks — run once at import time so individual tests don't
# each re-probe the kernel.
_IPV6_OK, _IPV6_REASON = _ipv6_available()
_IPV6_HOSTNAME = _find_ipv6_hostname() if _IPV6_OK else None


class MockServerV6:
    """
    Single-connection IPv6-only TCP server bound to ::1.
    Mirrors the interface of MockServer; IPV6_V6ONLY is set so it never
    accidentally accepts IPv4-mapped connections.
    """

    def __init__(self, handler):
        self.handler = handler
        self._sock   = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET,    socket.SO_REUSEADDR,  1)
        self._sock.setsockopt(socket.IPPROTO_IPV6,  socket.IPV6_V6ONLY,   1)
        self._sock.bind(("::1", 0))
        self._sock.listen(4)
        self._sock.settimeout(5.0)
        self.port    = self._sock.getsockname()[1]
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._stop   = threading.Event()

    def start(self):
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()
        try:
            self._sock.close()
        except OSError:
            pass

    def _serve(self):
        try:
            conn, _ = self._sock.accept()
        except OSError:
            return
        try:
            self.handler(conn)
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def url(self, path="/stream"):
        # RFC 2732 bracket notation for IPv6 literals in URLs.
        return f"http://[::1]:{self.port}{path}"

    def hostname_url(self, hostname, path="/stream"):
        """URL using a hostname that resolves to ::1 (avoids bracket-notation
        parsing issues and tests the DNS path instead)."""
        return f"http://{hostname}:{self.port}{path}"


class IPv6Tests(SikradioTestBase):
    """
    Testy obsługi protokołu IPv6.

    Cała klasa jest pomijana gdy:
      - jądro nie ma AF_INET6 (errno EAFNOSUPPORT — np. kontener bez modułu)
      - ::1 nie jest dostępny (brak interfejsu lo z IPv6)

    Testy, które wymagają rozwiązania nazwy hosta na AAAA (ip6-localhost /
    localhost -> ::1), są pomijane dodatkowo gdy żaden ze standardowych wpisów
    /etc/hosts nie zwraca adresu IPv6.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        if not _IPV6_OK:
            raise unittest.SkipTest(
                f"IPv6 not available on this host: {_IPV6_REASON}"
            )

    # -------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------

    def _require_v6_hostname(self):
        """Skip this test if no hostname resolves to AAAA here."""
        if _IPV6_HOSTNAME is None:
            self.skipTest(
                "No hostname resolves to IPv6 on this host "
                "(no '::1 ip6-localhost' or '::1 localhost' in /etc/hosts)"
            )
        return _IPV6_HOSTNAME

    # -------------------------------------------------------------------
    # Basic connectivity over IPv6
    # -------------------------------------------------------------------

    def test_ipv6_flag_connects_and_streams(self):
        """
        -6 musi wymusić połączenie IPv6; klient odbiera audio poprawnie.
        Serwer nasłuchuje WYŁĄCZNIE na ::1 (IPV6_V6ONLY), więc test
        upadnie, jeśli klient spróbuje IPv4.
        """
        payload = b"IPV6_AUDIO"

        def handler(conn):
            read_request(conn)
            http_200(conn, body=payload)

        srv = MockServerV6(handler).start()
        self.addCleanup(srv.stop)

        hostname = self._require_v6_hostname()
        args = ["-u", srv.hostname_url(hostname), "-6", "-t", "3000"]
        rc, out, err = self.launch_client(args)

        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertEqual(0, r,
                             f"rc={r}, stderr={e.decode(errors='replace')!r}"),
            self.assertIn(payload, o),
        ))

    def test_ipv6_audio_bytes_not_corrupted(self):
        """Wszystkie wartości bajtów (0–255) muszą przejść przez IPv6 bez korupcji."""
        payload = bytes(range(256)) * 8   # 2 KiB

        def handler(conn):
            read_request(conn)
            http_200(conn, body=payload)

        srv = MockServerV6(handler).start()
        self.addCleanup(srv.stop)

        hostname = self._require_v6_hostname()
        args = ["-u", srv.hostname_url(hostname), "-6", "-t", "3000"]
        rc, out, err = self.launch_client(args)

        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertEqual(0, r),
            self.assertEqual(payload, o),
        ))

    def test_ipv6_server_close_exits_zero(self):
        """EOF od serwera IPv6 -> exit 0 (identyczne z IPv4)."""
        def handler(conn):
            read_request(conn)
            http_200(conn, body=b"DONE")

        srv = MockServerV6(handler).start()
        self.addCleanup(srv.stop)

        hostname = self._require_v6_hostname()
        args = ["-u", srv.hostname_url(hostname), "-6", "-t", "2000"]
        rc, out, err = self.launch_client(args)

        self.assertTestPasses(rc, out, err, args, lambda r, o, e: self.assertEqual(0, r))

    # -------------------------------------------------------------------
    # getaddrinfo filtering
    # -------------------------------------------------------------------

    def test_ipv6_flag_shown_in_protocol_log(self):
        """
        Z -6 i -v1 stderr musi zawierać adres IPv6 serwera w notacji nawiasowej
        (np. '[::1]'), potwierdzając, że klient wybrał adres IPv6 z getaddrinfo.
        """
        def handler(conn):
            read_request(conn)
            http_200(conn, body=b"X")

        srv = MockServerV6(handler).start()
        self.addCleanup(srv.stop)

        hostname = self._require_v6_hostname()
        args = ["-u", srv.hostname_url(hostname), "-6", "-v1", "-t", "2000"]
        rc, out, err = self.launch_client(args)

        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertEqual(0, r),
            self.assertIn(b"::1", e,
                          f"Expected IPv6 address in protocol log, stderr={e.decode(errors='replace')!r}"),
        ))

    def test_ipv6_only_server_rejects_ipv4_flag(self):
        """
        Serwer nasłuchuje wyłącznie na ::1; klient z -4 musi dostać rc=1
        (getaddrinfo z AF_INET nie zwróci adresu IPv6, lub connect() odmówi).
        """
        def handler(conn):
            read_request(conn)
            http_200(conn, body=b"X")

        srv = MockServerV6(handler).start()
        self.addCleanup(srv.stop)

        hostname = self._require_v6_hostname()
        args = ["-u", srv.hostname_url(hostname), "-4", "-t", "500"]
        rc, out, err = self.launch_client(args)

        self.assertTestPasses(rc, out, err, args, lambda r, o, e: self.assertEqual(1, r))

    def test_ipv6_getaddrinfo_uses_ipv6_family(self):
        """
        Weryfikuje po stronie serwera, że połączenie przyszło z gniazda
        AF_INET6 (conn.family), nie IPv4-mapped.  Gwarantuje, że flaga -6
        rzeczywiście przechodzi do getaddrinfo jako AF_INET6, a nie AF_UNSPEC.
        """
        peer = {}

        def handler(conn):
            peer["family"] = conn.family
            read_request(conn)
            http_200(conn, body=b"X")

        srv = MockServerV6(handler).start()
        self.addCleanup(srv.stop)

        hostname = self._require_v6_hostname()
        args = ["-u", srv.hostname_url(hostname), "-6", "-t", "2000"]
        rc, out, err = self.launch_client(args)

        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertEqual(0, r),
            self.assertEqual(socket.AF_INET6, peer.get("family"),
                             "Server received connection on non-IPv6 socket"),
        ))

    # -------------------------------------------------------------------
    # Bare IPv6 address literal (RFC 2732 bracket notation)
    # -------------------------------------------------------------------

    def test_bare_ipv6_literal_bracket_notation(self):
        """
        URL z literałem IPv6: http://[::1]:PORT/
        RFC 2732 wymaga nawiasów w URL dla adresów IPv6.
        parse_url musi poprawnie oddzielić '[::1]' od ':PORT' i rozwiązać
        adres przez getaddrinfo bez usuwania nawiasów.
        """
        payload = b"LITERAL_IPV6_OK"

        def handler(conn):
            read_request(conn)
            http_200(conn, body=payload)

        srv = MockServerV6(handler).start()
        self.addCleanup(srv.stop)

        # srv.url() = http://[::1]:PORT/stream
        args = ["-u", srv.url(), "-6", "-t", "3000"]
        rc, out, err = self.launch_client(args)

        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertEqual(0, r,
                             "parse_url must handle RFC 2732 [::1] bracket notation; "
                             f"stderr={e.decode(errors='replace')!r}"),
            self.assertIn(payload, o),
        ))

    def test_host_header_bracket_notation_for_ipv6_literal(self):
        """
        Nagłówek Host dla adresu IPv6 musi być w formacie [::1]:PORT (RFC 2732).
        Weryfikujemy bezpośrednio treść żądania odebranego przez serwer.
        """
        received = {}

        def handler(conn):
            received["req"] = read_request(conn)
            http_200(conn, body=b"X")

        srv = MockServerV6(handler).start()
        self.addCleanup(srv.stop)

        args = ["-u", srv.url(), "-6", "-t", "2000"]
        rc, out, err = self.launch_client(args)

        expected_host = f"[::1]:{srv.port}"
        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertEqual(0, r),
            self.assertIn(expected_host, received.get("req", ""),
                          f"Expected 'Host: {expected_host}' in request, "
                          f"got: {received.get('req', '')!r}"),
        ))

    # -------------------------------------------------------------------
    # IPv6 connection failure
    # -------------------------------------------------------------------

    def test_ipv6_refused_connection_exits_one(self):
        """
        Połączenie z niezajętym portem na ::1 -> ECONNREFUSED -> rc=1.
        Analogon test_connection_refused_exits_one z IPv4.
        """
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("::1", 0))
            port = s.getsockname()[1]
        # Port is now free (socket closed) — connect() will be refused.

        hostname = self._require_v6_hostname()
        args = ["-u", f"http://{hostname}:{port}/", "-6", "-t", "200"]
        rc, out, err = self.launch_client(args)

        self.assertTestPasses(rc, out, err, args, lambda r, o, e: (
            self.assertEqual(1, r),
            self.assertNotEqual(-signal.SIGSEGV, r),
        ))


# ===========================================================================
# Runner
# ===========================================================================

if __name__ == "__main__":
    try:
        make = subprocess.run(["make"], check=True)

    except FileNotFoundError:
        print("make not found", file=sys.stderr)
        sys.exit(1)

    except subprocess.CalledProcessError as e:
        print(f"make failed: {e}", file=sys.stderr)
        sys.exit(e.returncode)

    unittest.main(verbosity=2)
