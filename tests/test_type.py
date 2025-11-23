import time

from .utils import exec_command


def test_type_reports_none_for_missing_key(master, make_key):
    with master.client() as conn:
        key = make_key("type_missing")
        assert exec_command(conn, "TYPE", key) == "none"


def test_type_reports_string(master, make_key):
    with master.client() as conn:
        key = make_key("type_string")
        assert exec_command(conn, "SET", key, "value") == "OK"
        assert exec_command(conn, "TYPE", key) == "string"


def test_type_reports_list(master, make_key):
    with master.client() as conn:
        key = make_key("type_list")
        assert exec_command(conn, "LPUSH", key, "a") == 1
        assert exec_command(conn, "TYPE", key) == "list"


def test_type_reflects_expired_strings(master, make_key):
    with master.client() as conn:
        key = make_key("type_expire")
        assert exec_command(conn, "SET", key, "value", "PX", "50") == "OK"
        time.sleep(0.08)
        assert exec_command(conn, "TYPE", key) == "none"
