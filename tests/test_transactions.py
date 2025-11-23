import pytest

from .utils import exec_command


def test_multi_returns_ok(master):
    with master.client() as conn:
        assert exec_command(conn, "MULTI") == "OK"


def test_exec_without_multi_errors(master):
    with master.client() as conn:
        with pytest.raises(RuntimeError) as excinfo:
            exec_command(conn, "EXEC")
        assert str(excinfo.value) == "ERR EXEC without MULTI"


def test_empty_transaction_executes_and_resets(master):
    with master.client() as conn:
        assert exec_command(conn, "MULTI") == "OK"
        assert exec_command(conn, "EXEC") == []

        with pytest.raises(RuntimeError) as excinfo:
            exec_command(conn, "EXEC")
        assert str(excinfo.value) == "ERR EXEC without MULTI"


def test_commands_are_queued_until_exec(master, make_key):
    with master.client() as conn:
        key = make_key("queued")

        assert exec_command(conn, "MULTI") == "OK"
        assert exec_command(conn, "SET", key, "41") == "QUEUED"
        assert exec_command(conn, "INCR", key) == "QUEUED"

        with master.client() as other:
            assert exec_command(other, "GET", key) is None

        assert isinstance(exec_command(conn, "EXEC"), list)


def test_exec_runs_all_queued_commands_and_returns_results(master, make_key):
    with master.client() as conn:
        foo = make_key("foo")
        bar = make_key("bar")

        assert exec_command(conn, "MULTI") == "OK"
        assert exec_command(conn, "SET", foo, "6") == "QUEUED"
        assert exec_command(conn, "INCR", foo) == "QUEUED"
        assert exec_command(conn, "INCR", bar) == "QUEUED"
        assert exec_command(conn, "GET", bar) == "QUEUED"

        assert exec_command(conn, "EXEC") == ["OK", 7, 1, "1"]

        with master.client() as other:
            assert exec_command(other, "GET", foo) == "7"


def test_nested_multi_errors(master):
    with master.client() as conn:
        assert exec_command(conn, "MULTI") == "OK"
        with pytest.raises(RuntimeError) as excinfo:
            exec_command(conn, "MULTI")
        assert str(excinfo.value) == "ERR MULTI calls can not be nested"


def test_discard_aborts_transaction(master, make_key):
    with master.client() as conn:
        key = make_key("discard")

        assert exec_command(conn, "MULTI") == "OK"
        assert exec_command(conn, "SET", key, "41") == "QUEUED"
        assert exec_command(conn, "INCR", key) == "QUEUED"
        assert exec_command(conn, "DISCARD") == "OK"
        assert exec_command(conn, "GET", key) is None

        with pytest.raises(RuntimeError) as excinfo:
            exec_command(conn, "DISCARD")
        assert str(excinfo.value) == "ERR DISCARD without MULTI"


def test_exec_collects_errors_and_continues(master, make_key):
    with master.client() as conn:
        foo = make_key("foo")
        bar = make_key("bar")

        assert exec_command(conn, "SET", foo, "abc") == "OK"
        assert exec_command(conn, "SET", bar, "41") == "OK"

        assert exec_command(conn, "MULTI") == "OK"
        assert exec_command(conn, "INCR", foo) == "QUEUED"
        assert exec_command(conn, "INCR", bar) == "QUEUED"

        result = exec_command(conn, "EXEC")
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[1] == 42
        assert isinstance(result[0], RuntimeError)
        assert str(result[0]) == "ERR value is not an integer or out of range"

        assert exec_command(conn, "GET", foo) == "abc"
        assert exec_command(conn, "GET", bar) == "42"


def test_multiple_transactions_have_separate_queues(master, make_key):
    with master.client() as conn:
        key = make_key("multi_clients")

        assert exec_command(conn, "MULTI") == "OK"
        assert exec_command(conn, "SET", key, "41") == "QUEUED"

        with master.client() as other:
            assert exec_command(other, "MULTI") == "OK"
            assert exec_command(other, "INCR", key) == "QUEUED"

            assert exec_command(conn, "INCR", key) == "QUEUED"
            assert exec_command(conn, "EXEC") == ["OK", 42]

            assert exec_command(other, "EXEC") == [43]
