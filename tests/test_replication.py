import socket
import threading
import time
from .utils import encode_command, read_reply, Connection


def test_replica_handshake_sends_commands(server_factory):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as fake_master:
        fake_master.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        fake_master.bind(("127.0.0.1", 0))
        fake_master.listen(1)
        port = fake_master.getsockname()[1]

        server_factory(args=["--replicaof", f"127.0.0.1 {port}"])

        conn, _ = fake_master.accept()
        with conn:
            replica = Connection(conn)

            assert replica.read_resp() == ["PING"]

            cmd = replica.read_resp()
            assert cmd[0] == "REPLCONF"
            assert cmd[1] == "listening-port"

            cmd = replica.read_resp()
            assert cmd[0] == "REPLCONF"
            assert cmd[1] == "capa"

            assert replica.read_resp() == ["PSYNC", "?", "-1"]


def test_master_handshake_responses(master):
    with master.client() as s:
        replica = Connection(s)

        s.sendall(encode_command("PING"))
        assert replica.read_resp() == "PONG"

        s.sendall(encode_command("REPLCONF", "listening-port", "6380"))
        assert replica.read_resp() == "OK"

        s.sendall(encode_command("REPLCONF", "capa", "psync2"))
        assert replica.read_resp() == "OK"

        s.sendall(encode_command("PSYNC", "?", "-1"))

        resp = replica.read_resp()
        assert resp.startswith("FULLRESYNC")

        rdb = replica.read_rdb()
        assert rdb.startswith(b"REDIS")


def test_master_propagates_writes(master):
    with master.client() as s:
        replica = _complete_handshake(s)

        with master.client() as client:
            client.sendall(encode_command("SET", "foo", "1"))
            assert read_reply(client) == "OK"
            client.sendall(encode_command("SET", "bar", "2"))
            assert read_reply(client) == "OK"
            client.sendall(encode_command("SET", "baz", "3"))
            assert read_reply(client) == "OK"

        assert replica.read_resp() == ["SET", "foo", "1"]
        assert replica.read_resp() == ["SET", "bar", "2"]
        assert replica.read_resp() == ["SET", "baz", "3"]


def test_master_propagates_writes_to_multiple_replicas(master):
    with master.client() as first, master.client() as second:
        replica_one = _complete_handshake(first)
        replica_two = _complete_handshake(second)

        with master.client() as client:
            client.sendall(encode_command("SET", "foo", "1"))
            assert read_reply(client) == "OK"
            client.sendall(encode_command("SET", "bar", "2"))
            assert read_reply(client) == "OK"
            client.sendall(encode_command("SET", "baz", "3"))
            assert read_reply(client) == "OK"

        for replica in (replica_one, replica_two):
            assert replica.read_resp() == ["SET", "foo", "1"]
            assert replica.read_resp() == ["SET", "bar", "2"]
            assert replica.read_resp() == ["SET", "baz", "3"]


def test_master_propagates_rpush_blpop_order(master):
    with master.client() as s:
        replica = _complete_handshake(s)

        t1 = threading.Thread(target=lambda: _blocking_client(master.port, "key"))
        t1.start()
        time.sleep(0.1)

        with master.client() as client:
            client.sendall(encode_command("RPUSH", "key", "val"))
            assert read_reply(client) == 1

        t1.join()

        resp1 = replica.read_resp()
        assert resp1 == ["RPUSH", "key", "val"]

        resp2 = replica.read_resp()
        assert resp2 == ["LPOP", "key"]


def test_replica_responds_to_getack(server_factory):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as master_sock:
        master_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        master_sock.bind(("127.0.0.1", 0))
        master_sock.listen(1)
        port = master_sock.getsockname()[1]

        server_factory(args=["--replicaof", f"127.0.0.1 {port}"])

        conn, _ = master_sock.accept()
        with conn:
            replica = Connection(conn)

            # Handshake
            assert replica.read_resp() == ["PING"]
            conn.sendall(b"+PONG\r\n")

            assert replica.read_resp()[0] == "REPLCONF"
            conn.sendall(b"+OK\r\n")

            assert replica.read_resp()[0] == "REPLCONF"
            conn.sendall(b"+OK\r\n")

            assert replica.read_resp()[0] == "PSYNC"
            conn.sendall(b"+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990ae25 0\r\n")

            empty_rdb = bytes.fromhex(
                "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bf220215a"
            )
            conn.sendall(
                b"$" + str(len(empty_rdb)).encode() + b"\r\n" + empty_rdb + b"\r\n"
            )

            conn.sendall(encode_command("REPLCONF", "GETACK", "*"))
            assert replica.read_resp() == ["REPLCONF", "ACK", "0"]

            conn.sendall(encode_command("PING"))

            conn.sendall(encode_command("REPLCONF", "GETACK", "*"))
            assert replica.read_resp() == ["REPLCONF", "ACK", "51"]

            conn.sendall(encode_command("SET", "foo", "1"))
            conn.sendall(encode_command("SET", "bar", "2"))

            conn.sendall(encode_command("REPLCONF", "GETACK", "*"))
            assert replica.read_resp() == ["REPLCONF", "ACK", "146"]


def test_replica_processes_propagated_commands(cluster):
    master = cluster.master
    replica = cluster.replicas[0]

    with master.client() as client:
        client.sendall(encode_command("SET", "foo", "bar"))
        assert read_reply(client) == "OK"

    with replica.client() as client:
        val = _wait_for_key(client, "foo", "bar")
        assert val == "bar"


def test_replica_processes_transactions(cluster):
    master = cluster.master
    replica = cluster.replicas[0]

    with master.client() as client:
        client.sendall(encode_command("MULTI"))
        assert read_reply(client) == "OK"
        client.sendall(encode_command("SET", "tx_key", "tx_val"))
        assert read_reply(client) == "QUEUED"
        client.sendall(encode_command("EXEC"))
        read_reply(client)

    with replica.client() as client:
        val = _wait_for_key(client, "tx_key", "tx_val")
        assert val == "tx_val"


def test_replica_processes_blocking_effects(cluster):
    master = cluster.master
    replica = cluster.replicas[0]

    t1 = threading.Thread(target=lambda: _blocking_client(master.port, "mylist"))
    t1.start()

    with master.client() as client:
        client.sendall(encode_command("RPUSH", "mylist", "item"))
        assert read_reply(client) == 1

    t1.join()

    with replica.client() as client:
        client.sendall(encode_command("LLEN", "mylist"))
        assert read_reply(client) == 0


def _wait_for_key(client, key, expected_val, timeout=2.0):
    start = time.time()
    while time.time() - start < timeout:
        client.sendall(encode_command("GET", key))
        val = read_reply(client)
        if val == expected_val:
            return val
        time.sleep(0.05)
    return None


def _blocking_client(port, key):
    try:
        with socket.create_connection(("127.0.0.1", port)) as s:
            s.sendall(encode_command("BLPOP", key, "0"))
            read_reply(s)
    except Exception:
        pass


def _complete_handshake(sock: socket.socket) -> Connection:
    replica = Connection(sock)

    sock.sendall(encode_command("PING"))
    assert replica.read_resp() == "PONG"

    sock.sendall(encode_command("REPLCONF", "listening-port", "6380"))
    assert replica.read_resp() == "OK"

    sock.sendall(encode_command("REPLCONF", "capa", "psync2"))
    assert replica.read_resp() == "OK"

    sock.sendall(encode_command("PSYNC", "?", "-1"))
    resp = replica.read_resp()
    assert resp.startswith("FULLRESYNC")

    replica.read_rdb()
    return replica


def test_replica_single_connection(server_factory):
    server_sock = None
    try:
        server_sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "IPV6_V6ONLY"):
            server_sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        server_sock.bind(("", 0))
    except Exception:
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind(("", 0))

    server_sock.listen(5)
    port = server_sock.getsockname()[1]

    server_factory(args=["--replicaof", f"localhost {port}"])

    server_sock.settimeout(5.0)
    connections = []

    start = time.time()
    while time.time() - start < 5.0:
        try:
            conn, addr = server_sock.accept()
            connections.append(conn)
            server_sock.settimeout(1.0)
        except socket.timeout:
            if len(connections) > 0:
                break
            continue
        except Exception:
            break

    for c in connections:
        c.close()
    server_sock.close()

    assert len(connections) == 1, f"Expected 1 connection, got {len(connections)}"
