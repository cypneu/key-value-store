import itertools
import shutil
import socket
import subprocess
import time
import tempfile
from pathlib import Path
from typing import NamedTuple

import pytest
from .utils import encode_command, read_reply

ROOT = Path(__file__).resolve().parents[1]
BIN = ROOT / "zig-out" / "bin" / "main"
_KEY_COUNTER = itertools.count()


class RedisServer:
    def __init__(self, proc: subprocess.Popen, port: int, stderr_file):
        self.proc = proc
        self.port = port
        self.host = "127.0.0.1"
        self.stderr_file = stderr_file

    def client(self, timeout: float = 2.0) -> socket.socket:
        s = socket.create_connection((self.host, self.port), timeout=timeout)
        return s

    def close(self):
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=1)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()

        # Check for memory leaks
        if self.proc.returncode != 0:
            self.stderr_file.seek(0)
            stderr_output = self.stderr_file.read().decode("utf-8", errors="replace")

            if "Memory leak detected" in stderr_output:
                raise RuntimeError(
                    f"Memory leak detected in server on port {self.port}!\nLogs:\n{stderr_output}"
                )

    def cleanup(self):
        self.close()
        self.stderr_file.close()


class RedisCluster(NamedTuple):
    master: RedisServer
    replicas: list[RedisServer]


@pytest.fixture(scope="session", autouse=True)
def build_binary():
    zig = shutil.which("zig")
    if zig is None:
        pytest.skip("'zig' not found in PATH")
    subprocess.run([zig, "build"], cwd=str(ROOT), check=True)


@pytest.fixture
def server_factory():
    servers: list[RedisServer] = []

    def _spawn(args: list[str] = None) -> RedisServer:
        port = _find_free_port()
        cmd = [str(BIN), "--port", str(port)] + (args or [])

        # Use a temporary file for stderr to avoid buffer filling issues
        stderr_file = tempfile.TemporaryFile()

        proc = subprocess.Popen(cmd, cwd=str(ROOT), stderr=stderr_file)
        server = RedisServer(proc, port, stderr_file)
        servers.append(server)

        try:
            _wait_for_port(server.host, server.port, time.time() + 5.0)
        except Exception:
            server.cleanup()
            raise

        return server

    yield _spawn

    for server in servers:
        server.cleanup()


@pytest.fixture
def master(server_factory) -> RedisServer:
    return server_factory()


@pytest.fixture
def cluster(request, server_factory) -> RedisCluster:
    config = getattr(request, "param", {})
    num_replicas = config.get("replicas", 1)

    master_server = server_factory()

    replica_servers = []
    for _ in range(num_replicas):
        replica = server_factory(
            args=["--replicaof", f"{master_server.host} {master_server.port}"]
        )
        replica_servers.append(replica)

    for replica in replica_servers:
        _wait_for_replica_ready(replica)

    return RedisCluster(master=master_server, replicas=replica_servers)


@pytest.fixture
def make_key():
    def _factory(prefix: str) -> str:
        return f"t_{{prefix}}_{next(_KEY_COUNTER)}"

    return _factory


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_port(host: str, port: int, deadline: float) -> None:
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.02)
    raise TimeoutError(f"Timed out waiting for {host}:{port} to be ready")


def _wait_for_replica_ready(replica: RedisServer, timeout: float = 5.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with replica.client(timeout=0.5) as client:
                client.sendall(encode_command("GET", "__replica_ready_probe"))
                read_reply(client)
                return
        except RuntimeError as e:
            if "LOADING" not in str(e):
                raise
        time.sleep(0.05)
    raise TimeoutError(f"Timed out waiting for replica on port {replica.port} to load")
