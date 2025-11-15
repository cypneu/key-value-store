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

    with pytest.raises(RuntimeError, match="ERR The ID specified in XADD must be greater than 0-0"):
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
