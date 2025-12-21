import socket
import queue
import threading
import time
from .utils import encode_command, read_reply, Connection, exec_command


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


def test_psync_fullresync_transfers_data(server_factory):
    master = server_factory()

    with master.client() as client:
        assert exec_command(client, "SET", "snap_string", "value") == "OK"
        assert exec_command(client, "RPUSH", "snap_list", "a", "b") == 2
        assert (
            exec_command(client, "XADD", "snap_stream", "*", "foo", "bar") is not None
        )

    replica = server_factory(args=["--replicaof", f"{master.host} {master.port}"])

    with replica.client() as client:
        val = _wait_for_key(client, "snap_string", "value", timeout=3.0)
        assert val == "value"

        client.sendall(encode_command("LRANGE", "snap_list", "0", "-1"))
        assert read_reply(client) == ["a", "b"]

        client.sendall(encode_command("XRANGE", "snap_stream", "-", "+"))
        stream_reply = read_reply(client)
        assert isinstance(stream_reply, list)
        assert len(stream_reply) == 1
        assert stream_reply[0][1] == ["foo", "bar"]


def test_psync_preserves_expiry(server_factory):
    master = server_factory()

    ttl_ms = 3000
    with master.client() as client:
        assert (
            exec_command(client, "SET", "expiring_key", "ephemeral", "PX", str(ttl_ms))
            == "OK"
        )

    replica = server_factory(args=["--replicaof", f"{master.host} {master.port}"])

    with replica.client() as client:
        val = _wait_for_key(client, "expiring_key", "ephemeral", timeout=3.0)
        assert val == "ephemeral"

        time.sleep(1.0)
        client.sendall(encode_command("GET", "expiring_key"))
        assert read_reply(client) == "ephemeral"

        time.sleep(2.5)
        client.sendall(encode_command("GET", "expiring_key"))
        assert read_reply(client) is None


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


def test_concurrent_writes_during_snapshot(server_factory):
    master = server_factory()

    with master.client() as client:
        for i in range(50):
            exec_command(client, "SET", f"pre_{i}", f"val_{i}")

    stop_writing = threading.Event()
    write_errors = []

    def writer_thread():
        try:
            with master.client() as client:
                i = 0
                while not stop_writing.is_set():
                    exec_command(client, "SET", f"conc_{i}", f"val_{i}")
                    i += 1
                    if i % 10 == 0:
                        time.sleep(0.001)
        except Exception as e:
            write_errors.append(e)

    t = threading.Thread(target=writer_thread)
    t.start()

    time.sleep(0.1)
    replica = server_factory(args=["--replicaof", f"{master.host} {master.port}"])

    time.sleep(2.0)
    stop_writing.set()
    t.join()

    assert not write_errors, f"Writer thread failed: {write_errors}"

    with replica.client() as client:
        val = _wait_for_key(client, "pre_49", "val_49")
        assert val == "val_49", "Replica missed data from RDB snapshot"

        val = _wait_for_key(client, "conc_0", "val_0")
        assert val == "val_0", (
            "Replica missed concurrent writes buffered during snapshot"
        )


def test_large_dataset_transfer(server_factory):
    master = server_factory()

    payload = "x" * 100
    with master.client() as client:
        for i in range(1000):
            exec_command(client, "SET", f"large_{i}", payload)

    replica = server_factory(args=["--replicaof", f"{master.host} {master.port}"])

    with replica.client() as client:
        val = _wait_for_key(client, "large_999", payload, timeout=5.0)
        assert val == payload


def test_replica_reconnection_lifecycle(server_factory):
    master = server_factory()

    replica1 = server_factory(args=["--replicaof", f"{master.host} {master.port}"])
    with replica1.client() as c:
        assert _wait_for_key(c, "nonexistent", None) is None

    replica1.close()

    with master.client() as client:
        exec_command(client, "SET", "k1", "v1")

    replica2 = server_factory(args=["--replicaof", f"{master.host} {master.port}"])

    with replica2.client() as c:
        val = _wait_for_key(c, "k1", "v1")
        assert val == "v1"


def test_replica_handles_fragmented_master_stream(server_factory):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as master_sock:
        master_sock.bind(("127.0.0.1", 0))
        master_sock.listen(1)
        master_port = master_sock.getsockname()[1]

        replica_server = server_factory(
            args=["--replicaof", f"127.0.0.1 {master_port}"]
        )

        conn, _ = master_sock.accept()
        with conn:
            reader = Connection(conn)

            assert reader.read_resp() == ["PING"]
            conn.sendall(b"+PONG\r\n")

            assert reader.read_resp()[0] == "REPLCONF"
            conn.sendall(b"+OK\r\n")

            assert reader.read_resp()[0] == "REPLCONF"
            conn.sendall(b"+OK\r\n")

            assert reader.read_resp()[0] == "PSYNC"

            msg = b"+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"
            for i in range(0, len(msg), 3):
                conn.sendall(msg[i : i + 3])
                time.sleep(0.01)

            empty_rdb = bytes.fromhex(
                "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bf220215a"
            )

            header = b"$" + str(len(empty_rdb)).encode() + b"\r\n"
            conn.sendall(header[:2])
            time.sleep(0.01)
            conn.sendall(header[2:])

            payload_with_footer = empty_rdb + b"\r\n"
            chunk_size = 10
            for i in range(0, len(payload_with_footer), chunk_size):
                conn.sendall(payload_with_footer[i : i + chunk_size])
                time.sleep(0.005)

            cmd = encode_command("SET", "foo", "bar")
            conn.sendall(cmd[:5])
            time.sleep(0.01)
            conn.sendall(cmd[5:])

            with replica_server.client() as c:
                val = _wait_for_key(c, "foo", "bar")
                assert val == "bar", "Replica failed to parse fragmented command stream"


def _wait_for_key(client, key, expected_val, timeout=2.0):
    start = time.time()
    while time.time() - start < timeout:
        client.sendall(encode_command("GET", key))
        try:
            val = read_reply(client)
        except RuntimeError as e:
            if "LOADING" in str(e):
                time.sleep(0.05)
                continue
            raise
        if val == expected_val:
            return val
        time.sleep(0.05)
    return None


def test_replica_blpop_unblocks_when_master_sends_rpush(cluster):
    replica = cluster.replicas[0]

    blpop_result = queue.Queue()

    def blpop_on_replica():
        try:
            with socket.create_connection(("127.0.0.1", replica.port), timeout=5) as s:
                s.sendall(encode_command("BLPOP", "testkey", "5"))
                blpop_result.put(read_reply(s))
        except Exception as e:
            blpop_result.put(e)

    t = threading.Thread(target=blpop_on_replica)
    t.start()
    time.sleep(0.1)

    with cluster.master.client() as client:
        client.sendall(encode_command("RPUSH", "testkey", "value1"))
        assert read_reply(client) == 1

    t.join(timeout=3)

    assert not blpop_result.empty(), "BLPOP client did not receive a response"
    result = blpop_result.get(timeout=1)
    assert result == ["testkey", "value1"]


def test_all_replicas_get_data_from_snapshot(server_factory):
    master = server_factory()

    with master.client() as client:
        for i in range(50):
            exec_command(client, "SET", f"shared_{i}", f"val_{i}")

    replicas = []
    for _ in range(3):
        replica = server_factory(args=["--replicaof", f"{master.host} {master.port}"])
        replicas.append(replica)

    for idx, replica in enumerate(replicas):
        with replica.client() as c:
            val = _wait_for_key(c, "shared_49", "val_49", timeout=5.0)
            assert val == "val_49", f"Replica {idx} missed data from snapshot"
