import queue
import socket
import threading
import time

from .utils import exec_command


def test_lpush_and_lrange(conn, make_key):
    key = make_key("list_lr")
    assert exec_command(conn, "LPUSH", key, "a", "b", "c") == 3
    assert exec_command(conn, "LRANGE", key, "0", "-1") == ["c", "b", "a"]


def test_rpush_and_llen(conn, make_key):
    key = make_key("list_len")
    assert exec_command(conn, "RPUSH", key, "x", "y") == 2
    assert exec_command(conn, "LLEN", key) == 2


def test_lpop_single_and_multiple(conn, make_key):
    key = make_key("list_pop")
    assert exec_command(conn, "RPUSH", key, "one", "two", "three") == 3
    assert exec_command(conn, "LPOP", key) == "one"
    assert exec_command(conn, "LPOP", key, "2") == ["two", "three"]


def test_lpop_missing_key_returns_null(conn, make_key):
    key = make_key("list_missing")
    assert exec_command(conn, "LPOP", key) is None


def test_llen_missing_key_is_zero(conn, make_key):
    key = make_key("list_missing_len")
    assert exec_command(conn, "LLEN", key) == 0


def test_blpop_blocks_until_push(server_proc, make_key):
    key = make_key("list_blpop")
    replies = queue.Queue()

    def blpop_worker():
        with socket.create_connection(("127.0.0.1", 6379), timeout=2.0) as s:
            replies.put(exec_command(s, "BLPOP", key, "1.0"))

    worker = threading.Thread(target=blpop_worker)
    worker.start()

    time.sleep(0.1)

    with socket.create_connection(("127.0.0.1", 6379), timeout=2.0) as push_conn:
        assert exec_command(push_conn, "LPUSH", key, "z") == 1

    worker.join(timeout=2.0)
    assert not worker.is_alive(), "BLPOP worker did not finish in time"

    assert replies.get_nowait() == [key, "z"]


def test_rpush_after_waiter_consumed(conn, make_key):
    key = make_key("list_waiter_cleanup")
    replies = queue.Queue()

    def blpop_worker():
        with socket.create_connection(("127.0.0.1", 6379), timeout=2.0) as s:
            replies.put(exec_command(s, "BLPOP", key, "1"))

    worker = threading.Thread(target=blpop_worker)
    worker.start()

    time.sleep(0.05)
    assert exec_command(conn, "RPUSH", key, "first") == 1

    worker.join(timeout=2.0)
    assert not worker.is_alive(), "BLPOP worker did not finish in time"
    assert replies.get_nowait() == [key, "first"]

    assert exec_command(conn, "LLEN", key) == 0
    assert exec_command(conn, "RPUSH", key, "second") == 1
    assert exec_command(conn, "LLEN", key) == 1
