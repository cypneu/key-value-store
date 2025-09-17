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
