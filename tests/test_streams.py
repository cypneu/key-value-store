import queue
import socket
import threading
import time

import pytest

from .utils import exec_command


def test_xadd_returns_entry_id(conn, make_key):
    key = make_key("stream_id")
    entry_id = "0-1"

    assert exec_command(conn, "XADD", key, entry_id, "foo", "bar") == entry_id


def test_type_reports_stream_for_stream_key(conn, make_key):
    key = make_key("stream_type")
    exec_command(conn, "XADD", key, "42-0", "sensor", "value")

    assert exec_command(conn, "TYPE", key) == "stream"


def test_xadd_returns_wrongtype_for_existing_string(conn, make_key):
    key = make_key("stream_wrongtype_string")
    assert exec_command(conn, "SET", key, "value") == "OK"

    with pytest.raises(RuntimeError, match="WRONGTYPE"):
        exec_command(conn, "XADD", key, "1-0", "field", "value")


def test_xadd_returns_wrongtype_for_existing_list(conn, make_key):
    key = make_key("stream_wrongtype_list")
    assert exec_command(conn, "LPUSH", key, "one") == 1

    with pytest.raises(RuntimeError, match="WRONGTYPE"):
        exec_command(conn, "XADD", key, "1-0", "field", "value")


def test_xadd_rejects_non_incrementing_id(conn, make_key):
    key = make_key("stream_invalid_id")
    exec_command(conn, "XADD", key, "1-1", "foo", "bar")

    err_msg = "ERR The ID specified in XADD is equal or smaller than the target stream top item"
    with pytest.raises(RuntimeError, match=err_msg):
        exec_command(conn, "XADD", key, "1-1", "sensor", "value")

    with pytest.raises(RuntimeError, match=err_msg):
        exec_command(conn, "XADD", key, "0-5", "sensor", "value")


def test_xadd_rejects_zero_id(conn, make_key):
    key = make_key("stream_zero_id")

    with pytest.raises(
        RuntimeError, match="ERR The ID specified in XADD must be greater than 0-0"
    ):
        exec_command(conn, "XADD", key, "0-0", "sensor", "value")


def test_xadd_auto_generates_sequence(conn, make_key):
    key = make_key("stream_auto_sequence")

    assert exec_command(conn, "XADD", key, "0-*", "foo", "bar") == "0-1"
    assert exec_command(conn, "XADD", key, "5-*", "foo", "bar") == "5-0"
    assert exec_command(conn, "XADD", key, "5-*", "bar", "baz") == "5-1"


def test_xadd_auto_generates_full_id(conn, make_key):
    key = make_key("stream_auto_full")

    start_ms = int(time.time() * 1000)
    entry_id = exec_command(conn, "XADD", key, "*", "foo", "bar")
    end_ms = int(time.time() * 1000)

    ms_part, seq_part = entry_id.split("-")
    ts_ms = int(ms_part)
    seq = int(seq_part)

    assert start_ms <= ts_ms <= end_ms
    assert seq == 0


def test_xrange_returns_entries(conn, make_key):
    key = make_key("stream_xrange")

    assert exec_command(conn, "XADD", key, "0-1", "foo", "bar") == "0-1"
    assert exec_command(conn, "XADD", key, "0-2", "bar", "baz") == "0-2"
    assert exec_command(conn, "XADD", key, "0-3", "baz", "foo") == "0-3"

    assert exec_command(conn, "XRANGE", key, "0-2", "0-3") == [
        ["0-2", ["bar", "baz"]],
        ["0-3", ["baz", "foo"]],
    ]


def test_xrange_allows_dash_start(conn, make_key):
    key = make_key("stream_xrange_dash")

    assert exec_command(conn, "XADD", key, "0-1", "foo", "bar") == "0-1"
    assert exec_command(conn, "XADD", key, "0-2", "bar", "baz") == "0-2"
    assert exec_command(conn, "XADD", key, "0-3", "baz", "foo") == "0-3"

    assert exec_command(conn, "XRANGE", key, "-", "0-2") == [
        ["0-1", ["foo", "bar"]],
        ["0-2", ["bar", "baz"]],
    ]


def test_xrange_allows_plus_end(conn, make_key):
    key = make_key("stream_xrange_plus")

    assert exec_command(conn, "XADD", key, "0-1", "foo", "bar") == "0-1"
    assert exec_command(conn, "XADD", key, "0-2", "bar", "baz") == "0-2"
    assert exec_command(conn, "XADD", key, "0-3", "baz", "foo") == "0-3"

    assert exec_command(conn, "XRANGE", key, "0-2", "+") == [
        ["0-2", ["bar", "baz"]],
        ["0-3", ["baz", "foo"]],
    ]


def test_xread_returns_entries(conn, make_key):
    key = make_key("stream_xread")

    assert exec_command(conn, "XADD", key, "0-1", "foo", "bar") == "0-1"
    assert exec_command(conn, "XADD", key, "0-2", "bar", "baz") == "0-2"
    assert exec_command(conn, "XADD", key, "0-3", "baz", "foo") == "0-3"

    assert exec_command(conn, "XREAD", "STREAMS", key, "0-1") == [
        [
            key,
            [
                ["0-2", ["bar", "baz"]],
                ["0-3", ["baz", "foo"]],
            ],
        ]
    ]


def test_xread_returns_none_when_no_entries(conn, make_key):
    key = make_key("stream_xread_empty")

    assert exec_command(conn, "XADD", key, "0-1", "foo", "bar") == "0-1"

    assert exec_command(conn, "XREAD", "STREAMS", key, "0-1") is None


def test_xread_multiple_streams(conn, make_key):
    key1 = make_key("stream_xread_multi1")
    key2 = make_key("stream_xread_multi2")

    assert exec_command(conn, "XADD", key1, "0-1", "temperature", "95") == "0-1"
    assert exec_command(conn, "XADD", key2, "0-2", "humidity", "97") == "0-2"

    assert exec_command(conn, "XREAD", "STREAMS", key1, key2, "0-0", "0-1") == [
        [
            key1,
            [
                ["0-1", ["temperature", "95"]],
            ],
        ],
        [
            key2,
            [
                ["0-2", ["humidity", "97"]],
            ],
        ],
    ]


def test_xread_block_returns_entry(server_proc, make_key):
    key = make_key("stream_xread_block")
    replies = queue.Queue()

    with socket.create_connection(("127.0.0.1", 6379), timeout=2.0) as seed_conn:
        assert exec_command(seed_conn, "XADD", key, "0-1", "temperature", "96") == "0-1"

    def reader():
        with socket.create_connection(("127.0.0.1", 6379), timeout=2.0) as sock:
            replies.put(
                exec_command(sock, "XREAD", "BLOCK", "1000", "STREAMS", key, "0-1")
            )

    worker = threading.Thread(target=reader)
    worker.start()

    time.sleep(0.05)
    with socket.create_connection(("127.0.0.1", 6379), timeout=2.0) as writer:
        assert exec_command(writer, "XADD", key, "0-2", "temperature", "95") == "0-2"

    worker.join(timeout=2.0)
    assert not worker.is_alive(), "XREAD worker did not finish in time"

    assert replies.get_nowait() == [
        [
            key,
            [
                ["0-2", ["temperature", "95"]],
            ],
        ]
    ]


def test_xread_block_multiple_keys_returns_single_stream(server_proc, make_key):
    key1 = make_key("stream_xread_block_multi_1")
    key2 = make_key("stream_xread_block_multi_2")
    replies = queue.Queue()

    with socket.create_connection(("127.0.0.1", 6379), timeout=2.0) as seed_conn:
        assert exec_command(seed_conn, "XADD", key1, "0-1", "sensor", "a") == "0-1"
        assert exec_command(seed_conn, "XADD", key2, "0-1", "sensor", "b") == "0-1"

    def reader():
        with socket.create_connection(("127.0.0.1", 6379), timeout=2.0) as sock:
            replies.put(
                exec_command(
                    sock, "XREAD", "BLOCK", "1000", "STREAMS", key1, key2, "0-1", "0-1"
                )
            )

    worker = threading.Thread(target=reader)
    worker.start()

    time.sleep(0.05)
    with socket.create_connection(("127.0.0.1", 6379), timeout=2.0) as writer:
        assert exec_command(writer, "XADD", key2, "0-2", "sensor", "c") == "0-2"

    worker.join(timeout=2.0)
    assert not worker.is_alive(), "XREAD worker did not finish in time"
    assert replies.get_nowait() == [
        [
            key2,
            [
                ["0-2", ["sensor", "c"]],
            ],
        ]
    ]


def test_xread_block_zero_waits_indefinitely(server_proc, make_key):
    key = make_key("stream_xread_block_forever")
    replies = queue.Queue()

    with socket.create_connection(("127.0.0.1", 6379), timeout=2.0) as seed_conn:
        assert exec_command(seed_conn, "XADD", key, "0-1", "sensor", "1") == "0-1"

    def reader():
        with socket.create_connection(("127.0.0.1", 6379), timeout=2.0) as sock:
            replies.put(
                exec_command(sock, "XREAD", "BLOCK", "0", "STREAMS", key, "0-1")
            )

    worker = threading.Thread(target=reader)
    worker.start()

    time.sleep(0.3)
    assert worker.is_alive(), "XREAD BLOCK 0 should still be waiting"

    with socket.create_connection(("127.0.0.1", 6379), timeout=2.0) as writer:
        assert exec_command(writer, "XADD", key, "0-2", "sensor", "2") == "0-2"

    worker.join(timeout=2.0)
    assert not worker.is_alive(), "XREAD worker did not finish in time"

    assert replies.get_nowait() == [
        [
            key,
            [
                ["0-2", ["sensor", "2"]],
            ],
        ]
    ]


def test_xread_block_timeout_returns_null(server_proc, make_key):
    key = make_key("stream_xread_block_timeout")
    replies = queue.Queue()

    with socket.create_connection(("127.0.0.1", 6379), timeout=2.0) as seed_conn:
        assert exec_command(seed_conn, "XADD", key, "0-1", "temperature", "93") == "0-1"

    def reader():
        with socket.create_connection(("127.0.0.1", 6379), timeout=2.0) as sock:
            replies.put(
                exec_command(sock, "XREAD", "BLOCK", "200", "STREAMS", key, "0-1")
            )

    worker = threading.Thread(target=reader)
    worker.start()

    worker.join(timeout=2.0)
    assert not worker.is_alive(), "XREAD worker did not finish in time"
    assert replies.get_nowait() is None
