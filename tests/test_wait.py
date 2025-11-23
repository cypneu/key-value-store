import time

import pytest

from .utils import encode_command, read_reply


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

        client.sendall(encode_command("WAIT", "2", "500"))
        reply = read_reply(client)
        assert reply >= 2
