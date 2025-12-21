from .utils import encode_command, read_reply
import re


def test_info_replication_reports_master_role(master):
    with master.client() as s:
        s.sendall(encode_command("INFO", "replication"))
        reply = read_reply(s)
        assert "role:master" in reply
        assert "master_replid" in reply
        assert "master_repl_offset" in reply


def test_info_replication_reports_replica_role(cluster):
    replica = cluster.replicas[0]

    with replica.client() as s:
        s.sendall(encode_command("INFO", "replication"))
        reply = read_reply(s)
        assert "role:slave" in reply


def test_info_offset_increases_after_write(master):
    with master.client() as s:
        s.sendall(encode_command("INFO", "replication"))
        offset_before = _parse_info_offset(read_reply(s))

        s.sendall(encode_command("SET", "key1", "value1"))
        assert read_reply(s) == "OK"

        s.sendall(encode_command("INFO", "replication"))
        offset_after = _parse_info_offset(read_reply(s))

        assert offset_after > offset_before, (
            f"Offset should increase after write: before={offset_before}, after={offset_after}"
        )


def test_info_offset_increases_without_replicas(master):
    with master.client() as s:
        s.sendall(encode_command("INFO", "replication"))
        offset_before = _parse_info_offset(read_reply(s))

        for i in range(3):
            s.sendall(encode_command("SET", f"noreplica_key_{i}", f"value_{i}"))
            assert read_reply(s) == "OK"

        s.sendall(encode_command("INFO", "replication"))
        offset_after = _parse_info_offset(read_reply(s))

        assert offset_after > offset_before, (
            f"Offset should increase even without replicas: before={offset_before}, after={offset_after}"
        )


def _parse_info_offset(reply: str) -> int:
    match = re.search(r"master_repl_offset:(\d+)", reply)
    if not match:
        raise ValueError(f"Could not find master_repl_offset in: {reply}")
    return int(match.group(1))
