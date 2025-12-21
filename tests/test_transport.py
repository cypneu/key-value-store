import time

import pytest


from .utils import encode_command, exec_command, read_reply, send_in_chunks


CHUNK_DELAY = 0.005


def test_partial_command_delivery(master):
    with master.client() as conn:
        payload = encode_command("ECHO", "chunked")
        send_in_chunks(conn, payload, chunk_size=1, delay=CHUNK_DELAY)
        assert read_reply(conn) == "chunked"
        assert exec_command(conn, "PING") == "PONG"


def test_large_bulk_string_split_across_reads(master, make_key):
    with master.client() as conn:
        key = make_key("transport_large")
        value = "x" * 200_000

        assert exec_command(conn, "SET", key, value) == "OK"
        assert exec_command(conn, "GET", key) == value


def test_large_bulk_string_requires_multiple_writes(master, make_key):
    key = make_key("transport_write")
    value = "y" * (4 * 1024 * 1024)

    with master.client(timeout=3.0) as s:
        assert exec_command(s, "SET", key, value) == "OK"

        s.sendall(encode_command("GET", key))
        time.sleep(0.2)
        assert read_reply(s, timeout=5.0) == value


def test_protocol_error_closes_connection(master):
    with master.client() as conn:
        conn.sendall(b"x\r\n")
        with pytest.raises((RuntimeError, ConnectionError)):
            read_reply(conn, timeout=0.2)
