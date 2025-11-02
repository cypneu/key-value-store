import socket
import time


from .utils import encode_command, exec_command, read_reply, send_in_chunks


CHUNK_DELAY = 0.005


def test_partial_command_delivery(conn):
    payload = encode_command("ECHO", "chunked")
    send_in_chunks(conn, payload, chunk_size=1, delay=CHUNK_DELAY)
    assert read_reply(conn) == "chunked"
    assert exec_command(conn, "PING") == "PONG"


def test_large_bulk_string_split_across_reads(conn, make_key):
    key = make_key("transport_large")
    value = "x" * 200_000

    assert exec_command(conn, "SET", key, value) == "OK"
    assert exec_command(conn, "GET", key) == value


def test_large_bulk_string_requires_multiple_writes(server_proc, make_key):
    key = make_key("transport_write")
    value = "y" * (4 * 1024 * 1024)

    with socket.create_connection(("127.0.0.1", 6379), timeout=3.0) as s:
        assert exec_command(s, "SET", key, value) == "OK"

        s.sendall(encode_command("GET", key))
        time.sleep(0.2)
        assert read_reply(s, timeout=5.0) == value
