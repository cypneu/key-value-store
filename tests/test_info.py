from .utils import encode_command, read_reply


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
