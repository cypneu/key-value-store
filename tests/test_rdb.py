import pytest
from pathlib import Path
from .utils import encode_command, read_reply

ASSETS_DIR = Path(__file__).parent / "assets"


def test_rdb_load_many_small(server_factory):
    rdb_file = ASSETS_DIR / "many_small.rdb"
    if not rdb_file.exists():
        pytest.skip("many_small.rdb not found")

    server = server_factory(
        args=["--dir", str(ASSETS_DIR), "--dbfilename", "many_small.rdb"]
    )

    with server.client() as s:
        s.sendall(encode_command("GET", "str_0"))
        assert read_reply(s) == "val_0"
        s.sendall(encode_command("GET", "str_999"))
        assert read_reply(s) == "val_999"

        s.sendall(encode_command("LRANGE", "list_0", "0", "-1"))
        reply = read_reply(s)
        assert len(reply) == 10
        assert reply[0] == "item_0"

        s.sendall(encode_command("XRANGE", "stream_0", "-", "+"))
        reply = read_reply(s)
        assert len(reply) == 5

        assert reply[0][0] == "1000-0"
        assert reply[0][1] == ["field", "val_0"]


def test_rdb_load_few_large(server_factory):
    rdb_file = ASSETS_DIR / "few_large.rdb"
    if not rdb_file.exists():
        pytest.skip("few_large.rdb not found")

    server = server_factory(
        args=["--dir", str(ASSETS_DIR), "--dbfilename", "few_large.rdb"]
    )

    with server.client() as s:
        s.sendall(encode_command("GET", "large_str"))
        val = read_reply(s)
        assert len(val) == 1_000_000

        s.sendall(encode_command("LLEN", "large_list"))
        assert read_reply(s) == 10_000

        s.sendall(encode_command("XRANGE", "large_stream", "-", "+"))
        entries = read_reply(s)
        assert len(entries) == 5000


def test_rdb_non_existent_file(server_factory, tmp_path):
    rdb_dir = tmp_path / "redis-data"
    rdb_dir.mkdir()

    server = server_factory(args=["--dir", str(rdb_dir), "--dbfilename", "dump.rdb"])

    with server.client() as s:
        s.sendall(encode_command("KEYS", "*"))
        reply = read_reply(s)
        assert reply == []


def test_save_command(server_factory, tmp_path):
    rdb_dir = tmp_path / "redis-data-save"
    rdb_dir.mkdir()
    rdb_filename = "dump-save.rdb"

    server = server_factory(args=["--dir", str(rdb_dir), "--dbfilename", rdb_filename])

    with server.client() as s:
        s.sendall(encode_command("SET", "save_key", "save_val"))
        assert read_reply(s) == "OK"

        s.sendall(encode_command("SAVE"))
        assert read_reply(s) == "OK"

    assert (rdb_dir / rdb_filename).exists()

    server2 = server_factory(args=["--dir", str(rdb_dir), "--dbfilename", rdb_filename])
    with server2.client() as s:
        s.sendall(encode_command("GET", "save_key"))
        assert read_reply(s) == "save_val"

