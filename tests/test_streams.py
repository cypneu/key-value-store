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
