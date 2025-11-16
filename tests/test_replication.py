import socket
import subprocess

from .conftest import BIN, ROOT, _build_binary


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_replica_handshake_on_startup():
    _build_binary()

    master_port = _find_free_port()
    replica_port = _find_free_port()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as master:
        master.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        master.bind(("127.0.0.1", master_port))
        master.listen(1)

        proc = subprocess.Popen(
            [
                str(BIN),
                "--port",
                str(replica_port),
                "--replicaof",
                f"127.0.0.1 {master_port}",
            ],
            cwd=str(ROOT),
        )

        conn = None
        try:
            master.settimeout(3.0)
            conn, _ = master.accept()
            conn.settimeout(2.0)

            port_bytes = str(replica_port).encode()
            expected = b"".join(
                [
                    b"*1\r\n$4\r\nPING\r\n",
                    b"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$"
                    + str(len(port_bytes)).encode()
                    + b"\r\n"
                    + port_bytes
                    + b"\r\n",
                    b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n",
                    b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n",
                ]
            )

            received = b""
            while len(received) < len(expected):
                chunk = conn.recv(1024)
                if not chunk:
                    break
                received += chunk

            assert received == expected
        finally:
            if conn:
                try:
                    conn.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                conn.close()

            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    proc.kill()
