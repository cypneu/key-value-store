import time

import pytest

from .utils import encode_command, read_reply, Connection


def test_wait_returns_zero_without_replicas(master):
    with master.client() as client:
        client.sendall(encode_command("WAIT", "0", "60000"))
        reply = read_reply(client)
        assert reply == 0


@pytest.mark.parametrize("cluster", [{"replicas": 3}], indirect=True)
def test_wait_returns_connected_replica_count(cluster):
    expected = len(cluster.replicas)

    def wait_until_count(client):
        deadline = time.time() + 2.0
        while True:
            client.sendall(encode_command("WAIT", "0", "0"))
            reply = read_reply(client)
            if reply == expected:
                return
            if time.time() >= deadline:
                raise AssertionError(
                    f"replica count did not reach {expected}, last {reply}"
                )
            time.sleep(0.05)

    with cluster.master.client() as client:
        wait_until_count(client)
        for needed in ("3", "7", "9"):
            client.sendall(encode_command("WAIT", needed, "500"))
            assert read_reply(client) == expected


@pytest.mark.parametrize("cluster", [{"replicas": 2}], indirect=True)
def test_wait_after_write(cluster):
    with cluster.master.client() as client:
        client.sendall(encode_command("SET", "foo", "bar"))
        assert read_reply(client) == "OK"

        client.sendall(encode_command("WAIT", "2", "3000"))
        reply = read_reply(client)
        assert reply >= 2


def test_wait_timeout_reports_ack_count(server_factory):
    master = server_factory()
    replicas = [_connect_fake_replica(master) for _ in range(2)]

    try:
        with master.client() as client:
            client.sendall(encode_command("SET", "key", "value"))
            assert read_reply(client) == "OK"

            client.sendall(encode_command("WAIT", "3", "10"))
            reply = read_reply(client)
            assert reply == 0
    finally:
        for replica in replicas:
            replica.sock.close()


def _connect_fake_replica(master):
    sock = master.client()
    conn = Connection(sock)

    sock.sendall(encode_command("PING"))
    assert conn.read_resp() == "PONG"

    sock.sendall(encode_command("REPLCONF", "listening-port", "0"))
    assert conn.read_resp() == "OK"

    sock.sendall(encode_command("REPLCONF", "capa", "psync2"))
    assert conn.read_resp() == "OK"

    sock.sendall(encode_command("PSYNC", "?", "-1"))
    resp = conn.read_resp()
    assert isinstance(resp, str) and resp.startswith("FULLRESYNC")

    conn.read_rdb()
    return conn
