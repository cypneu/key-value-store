from .utils import exec_command


def test_info_replication_reports_master_role(conn):
    reply = exec_command(conn, "INFO", "replication")
    assert isinstance(reply, str)
    lines = [line for line in reply.split("\r\n") if line]
    assert "role:master" in lines
    replid_lines = [l for l in lines if l.startswith("master_replid:")]
    assert replid_lines
    replid_value = replid_lines[0].split(":", 1)[1]
    assert len(replid_value) == 40
    assert replid_value.isalnum()
    assert "master_repl_offset:0" in lines


def test_info_replication_reports_replica_role(conn_replica):
    reply = exec_command(conn_replica, "INFO", "replication")
    assert isinstance(reply, str)
    assert "role:slave" in reply
