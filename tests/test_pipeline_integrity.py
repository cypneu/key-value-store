import socket
import time
from .utils import encode_command


def test_pipeline_pauses_on_blocking_command(server_factory):
    server = server_factory()

    with server.client() as client_a, server.client() as client_b:
        client_a.sendall(encode_command("BLPOP", "key1", "0") + encode_command("PING"))

        _assert_client_blocked(client_a)

        client_b.sendall(encode_command("RPUSH", "key1", "val1"))
        assert b":1\r\n" in client_b.recv(1024)

        response = _assert_data_received(client_a, b"PONG")
        assert b"key1" in response
        assert b"val1" in response


def test_chained_blocking_pipeline(server_factory):
    server = server_factory()

    with server.client() as client_a, server.client() as client_b:
        client_a.sendall(
            encode_command("BLPOP", "key1", "0")
            + encode_command("BLPOP", "key2", "0")
            + encode_command("PING")
        )

        _assert_client_blocked(client_a)

        client_b.sendall(encode_command("RPUSH", "key1", "val1"))
        assert b":1\r\n" in client_b.recv(1024)

        data = _assert_data_received(client_a, b"val1")
        assert b"PONG" not in data

        _assert_client_blocked(client_a)

        client_b.sendall(encode_command("RPUSH", "key2", "val2"))
        assert b":1\r\n" in client_b.recv(1024)

        final_response = _assert_data_received(client_a, b"PONG")
        assert b"key2" in final_response
        assert b"val2" in final_response


def _assert_client_blocked(client):
    client.settimeout(0.2)
    try:
        data = client.recv(1024)
        assert not data, f"Protocol violation: Expected block, but received {data}"
    except socket.timeout:
        pass


def _assert_data_received(client, expected_bytes, timeout=2.0):
    client.settimeout(timeout)
    data = b""
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            chunk = client.recv(4096)
            if not chunk:
                break
            data += chunk
            if expected_bytes in data:
                return data
        except socket.timeout:
            break

    assert expected_bytes in data, f"Timeout: Expected {expected_bytes}, got {data}"
    return data
