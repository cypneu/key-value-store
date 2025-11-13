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
