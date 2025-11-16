import time

import pytest

from .utils import exec_command


def test_ping(conn):
    assert exec_command(conn, "PING") == "PONG"


def test_echo(conn):
    assert exec_command(conn, "ECHO", "hello") == "hello"


def test_set_and_get(conn, make_key):
    key = make_key("set_get")
    assert exec_command(conn, "SET", key, "world") == "OK"
    assert exec_command(conn, "GET", key) == "world"


def test_incr_on_existing_integer(conn, make_key):
    key = make_key("incr")
    assert exec_command(conn, "SET", key, "41") == "OK"
    assert exec_command(conn, "INCR", key) == 42
    assert exec_command(conn, "GET", key) == "42"


def test_incr_on_missing_sets_to_one(conn, make_key):
    key = make_key("incr_missing")
    assert exec_command(conn, "INCR", key) == 1
    assert exec_command(conn, "GET", key) == "1"
    other_key = make_key("incr_missing_second")
    assert exec_command(conn, "INCR", other_key) == 1


def test_incr_on_non_integer_value_returns_error(conn, make_key):
    key = make_key("incr_non_integer")
    assert exec_command(conn, "SET", key, "xyz") == "OK"
    with pytest.raises(RuntimeError, match="ERR value is not an integer or out of range"):
        exec_command(conn, "INCR", key)


def test_get_absent_returns_null(conn, make_key):
    key = make_key("missing")
    assert exec_command(conn, "GET", key) is None


@pytest.mark.parametrize("px_token", ["PX", "px"])
def test_set_with_px_expiry(conn, make_key, px_token):
    key = make_key("expiry")
    assert exec_command(conn, "SET", key, "v", px_token, "50") == "OK"
    time.sleep(0.08)
    assert exec_command(conn, "GET", key) is None


def test_set_overwrites_existing_list(conn, make_key):
    key = make_key("set_over_list")
    assert exec_command(conn, "LPUSH", key, "first") == 1
    assert exec_command(conn, "SET", key, "string") == "OK"
    assert exec_command(conn, "GET", key) == "string"
    with pytest.raises(RuntimeError, match="WRONGTYPE"):
        exec_command(conn, "LLEN", key)


def test_get_returns_wrongtype_for_list(conn, make_key):
    key = make_key("get_wrongtype_list")
    assert exec_command(conn, "LPUSH", key, "value") == 1
    with pytest.raises(RuntimeError, match="WRONGTYPE"):
        exec_command(conn, "GET", key)
