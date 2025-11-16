import itertools
import shutil
import socket
import subprocess
import time
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
BIN = ROOT / "zig-out" / "bin" / "main"
_KEY_COUNTER = itertools.count()
REPLICA_PORT = 6380


def _is_port_open(host: str, port: int, timeout: float = 0.1) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _wait_for_port(host: str, port: int, deadline: float) -> None:
    while time.time() < deadline:
        if _is_port_open(host, port):
            return
        time.sleep(0.02)
    raise TimeoutError(f"Timed out waiting for {host}:{port} to be ready")


def _build_binary() -> None:
    zig = shutil.which("zig")
    if zig is None:
        pytest.skip("'zig' not found in PATH; cannot build server binary")
    subprocess.run([zig, "build"], cwd=str(ROOT), check=True)


@pytest.fixture(scope="session")
def server_proc():
    _build_binary()

    if _is_port_open("127.0.0.1", 6379):
        pytest.skip("Port 6379 is already in use; cannot run integration tests")

    proc = subprocess.Popen([str(BIN)], cwd=str(ROOT))
    try:
        _wait_for_port("127.0.0.1", 6379, time.time() + 5.0)
    except Exception:
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()
        raise

    yield proc

    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()


@pytest.fixture(scope="session")
def server_proc_replica():
    _build_binary()

    if _is_port_open("127.0.0.1", REPLICA_PORT):
        pytest.skip(f"Port {REPLICA_PORT} is already in use; cannot run integration tests")

    proc = subprocess.Popen(
        [str(BIN), "--port", str(REPLICA_PORT), "--replicaof", "localhost 6379"],
        cwd=str(ROOT),
    )
    try:
        _wait_for_port("127.0.0.1", REPLICA_PORT, time.time() + 5.0)
    except Exception:
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()
        raise

    yield proc

    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()


@pytest.fixture()
def conn(server_proc):
    s = socket.create_connection(("127.0.0.1", 6379), timeout=2.0)
    try:
        yield s
    finally:
        try:
            s.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        s.close()


@pytest.fixture()
def conn_replica(server_proc_replica):
    s = socket.create_connection(("127.0.0.1", REPLICA_PORT), timeout=2.0)
    try:
        yield s
    finally:
        try:
            s.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        s.close()


@pytest.fixture()
def make_key():
    def _factory(prefix: str) -> str:
        return f"t_{prefix}_{next(_KEY_COUNTER)}"

    return _factory
