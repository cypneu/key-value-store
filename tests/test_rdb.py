import struct
import time
from .utils import encode_command, read_reply


def create_rdb_file(path, keys):
    # Header
    data = b"REDIS0011"

    # DB Selector
    data += b"\xfe\x00"

    # Resize DB
    data += b"\xfb" + struct.pack("B", len(keys)) + b"\x00"

    items = []
    if isinstance(keys, dict):
        for k, v in keys.items():
            items.append((k, v, None))
    else:
        items = keys

    for k, v, expiry in items:
        # Expiry
        if expiry is not None:
            # Use millisecond expiry (0xFC)
            data += b"\xfc" + struct.pack("<Q", expiry)

        # Type String (0)
        data += b"\x00"
        # Key
        data += struct.pack("B", len(k)) + k.encode()
        # Value
        data += struct.pack("B", len(v)) + v.encode()

    # EOF
    data += b"\xff"
    # Checksum (8 bytes)
    data += b"\x00" * 8

    with open(path, "wb") as f:
        f.write(data)


def test_rdb_load_single_key(server_factory, tmp_path):
    rdb_dir = tmp_path / "redis-data"
    rdb_dir.mkdir()
    rdb_file = "dump.rdb"
    rdb_path = rdb_dir / rdb_file

    create_rdb_file(rdb_path, {"foo": "bar"})

    server = server_factory(args=["--dir", str(rdb_dir), "--dbfilename", rdb_file])

    with server.client() as s:
        s.sendall(encode_command("KEYS", "*"))
        reply = read_reply(s)
        # Order is not guaranteed for KEYS *, but with one key it is.
        assert reply == ["foo"]

        s.sendall(encode_command("GET", "foo"))
        reply = read_reply(s)
        assert reply == "bar"


def test_rdb_load_multiple_keys(server_factory, tmp_path):
    rdb_dir = tmp_path / "redis-data"
    rdb_dir.mkdir()
    rdb_file = "dump.rdb"
    rdb_path = rdb_dir / rdb_file

    keys = {"key1": "val1", "key2": "val2", "key3": "val3"}
    create_rdb_file(rdb_path, keys)

    server = server_factory(args=["--dir", str(rdb_dir), "--dbfilename", rdb_file])

    with server.client() as s:
        s.sendall(encode_command("KEYS", "*"))
        reply = read_reply(s)
        assert isinstance(reply, list)
        assert len(reply) == 3
        assert set(reply) == set(keys.keys())

        for k, v in keys.items():
            s.sendall(encode_command("GET", k))
            assert read_reply(s) == v


def test_rdb_non_existent_file(server_factory, tmp_path):
    rdb_dir = tmp_path / "redis-data"
    rdb_dir.mkdir()

    server = server_factory(args=["--dir", str(rdb_dir), "--dbfilename", "dump.rdb"])

    with server.client() as s:
        s.sendall(encode_command("KEYS", "*"))
        reply = read_reply(s)
        assert reply == []


def test_rdb_expiry(server_factory, tmp_path):
    rdb_dir = tmp_path / "redis-data"
    rdb_dir.mkdir()
    rdb_file = "dump.rdb"
    rdb_path = rdb_dir / rdb_file

    now_ms = int(time.time() * 1000)
    past_ms = now_ms - 10000  # 10 seconds ago
    future_ms = now_ms + 10000  # 10 seconds in future

    items = [
        ("no_expiry", "val_no", None),
        ("expired", "val_exp", past_ms),
        ("active", "val_act", future_ms),
    ]

    create_rdb_file(rdb_path, items)

    server = server_factory(args=["--dir", str(rdb_dir), "--dbfilename", rdb_file])

    with server.client() as s:
        # Test no expiry
        s.sendall(encode_command("GET", "no_expiry"))
        reply = read_reply(s)
        assert reply == "val_no"

        # Test expired
        s.sendall(encode_command("GET", "expired"))
        reply = read_reply(s)
        assert reply is None  # Null bulk string

        # Test active (future expiry)
        s.sendall(encode_command("GET", "active"))
        reply = read_reply(s)
        assert reply == "val_act"
