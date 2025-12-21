import queue
import socket
import threading
import time

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


def test_exec_aborts_after_queue_error(master, make_key):
    with master.client() as conn:
        key = make_key("tx_abort")

        assert exec_command(conn, "MULTI") == "OK"

        with pytest.raises(RuntimeError) as excinfo:
            exec_command(conn, "SET", key)
        assert str(excinfo.value) == "ERR wrong number of arguments"

        assert exec_command(conn, "SET", key, "value") == "QUEUED"

        with pytest.raises(RuntimeError) as excinfo:
            exec_command(conn, "EXEC")
        assert (
            str(excinfo.value)
            == "EXECABORT Transaction discarded because of previous errors."
        )

        assert exec_command(conn, "GET", key) is None


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


def test_exec_unblocks_list_waiters(master, make_key):
    key = make_key("tx_blpop")
    replies = queue.Queue()

    def blpop_worker():
        try:
            with master.client() as s:
                replies.put(exec_command(s, "BLPOP", key, "1.0"))
        except Exception as exc:
            replies.put(exc)

    worker = threading.Thread(target=blpop_worker)
    worker.start()

    time.sleep(0.05)

    with master.client() as conn:
        assert exec_command(conn, "MULTI") == "OK"
        assert exec_command(conn, "LPUSH", key, "v") == "QUEUED"
        assert isinstance(exec_command(conn, "EXEC"), list)

    worker.join(timeout=2.0)
    assert not worker.is_alive(), "BLPOP worker did not finish in time"
    result = replies.get(timeout=1.0)
    assert result == [key, "v"]


def test_exec_blocking_command_does_not_emit_late_reply(master, make_key):
    with master.client() as conn:
        key = make_key("tx_blocking")
        assert exec_command(conn, "MULTI") == "OK"
        assert exec_command(conn, "BLPOP", key, "0.05") == "QUEUED"
        assert exec_command(conn, "EXEC") == [None]

        time.sleep(0.1)
        conn.settimeout(0.1)
        with pytest.raises(socket.timeout):
            conn.recv(1)


def test_exec_blpop_returns_value_when_available(master, make_key):
    with master.client() as conn:
        key = make_key("tx_blpop_ready")
        assert exec_command(conn, "LPUSH", key, "v") == 1
        assert exec_command(conn, "MULTI") == "OK"
        assert exec_command(conn, "BLPOP", key, "1") == "QUEUED"
        assert exec_command(conn, "EXEC") == [[key, "v"]]


def test_exec_xread_block_returns_entries(master, make_key):
    with master.client() as conn:
        key = make_key("tx_xread")
        exec_command(conn, "XADD", key, "*", "field", "value")

        assert exec_command(conn, "MULTI") == "OK"
        assert (
            exec_command(conn, "XREAD", "BLOCK", "50", "STREAMS", key, "0-0")
            == "QUEUED"
        )
        result = exec_command(conn, "EXEC")

        assert isinstance(result, list)
        assert len(result) == 1
        xread_reply = result[0]
        assert isinstance(xread_reply, list)
        assert xread_reply[0][0] == key
        entries = xread_reply[0][1]
        assert entries[0][1] == ["field", "value"]


def test_exec_xread_block_latest_returns_null(master, make_key):
    with master.client() as conn:
        key = make_key("tx_xread_latest")
        exec_command(conn, "XADD", key, "*", "field", "value")

        assert exec_command(conn, "MULTI") == "OK"
        assert (
            exec_command(conn, "XREAD", "BLOCK", "50", "STREAMS", key, "$") == "QUEUED"
        )
        assert exec_command(conn, "EXEC") == [None]


def test_exec_wait_returns_immediately(master):
    with master.client() as conn:
        assert exec_command(conn, "MULTI") == "OK"
        assert exec_command(conn, "WAIT", "1", "100") == "QUEUED"
        assert exec_command(conn, "EXEC") == [0]
