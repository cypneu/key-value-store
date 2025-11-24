from .utils import encode_command, read_reply


def test_config_get_dir(server_factory):
    directory = "/tmp/redis-files"
    server = server_factory(args=["--dir", directory, "--dbfilename", "dump.rdb"])

    with server.client() as s:
        s.sendall(encode_command("CONFIG", "GET", "dir"))
        reply = read_reply(s)
        assert reply == ["dir", directory]


def test_config_get_dbfilename(server_factory):
    dbfilename = "dump.rdb"
    server = server_factory(args=["--dir", "/tmp/redis-files", "--dbfilename", dbfilename])

    with server.client() as s:
        s.sendall(encode_command("CONFIG", "GET", "dbfilename"))
        reply = read_reply(s)
        assert reply == ["dbfilename", dbfilename]


def test_config_get_defaults(server_factory):
    server = server_factory()

    with server.client() as s:
        s.sendall(encode_command("CONFIG", "GET", "dir"))
        reply = read_reply(s)
        assert reply == ["dir", "."]

    with server.client() as s:
        s.sendall(encode_command("CONFIG", "GET", "dbfilename"))
        reply = read_reply(s)
        assert reply == ["dbfilename", "dump.rdb"]
