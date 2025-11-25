import socket
import time


CRLF = b"\r\n"


def exec_command(sock: socket.socket, *parts: bytes | str):
    sock.sendall(encode_command(*parts))
    return read_reply(sock)


def send_in_chunks(
    sock: socket.socket, payload: bytes, *, chunk_size: int, delay: float = 0.0
) -> None:
    for offset in range(0, len(payload), chunk_size):
        sock.sendall(payload[offset : offset + chunk_size])
        if delay:
            time.sleep(delay)


def encode_command(*parts: bytes | str) -> bytes:
    bparts = [p.encode() if isinstance(p, str) else p for p in parts]
    out = bytearray()
    out += b"*" + str(len(bparts)).encode() + CRLF
    for p in bparts:
        out += b"$" + str(len(p)).encode() + CRLF + p + CRLF
    return bytes(out)


def read_reply(sock: socket.socket, timeout: float = 2.0):
    sock.settimeout(timeout)
    reader = RESPReader()
    while True:
        parsed = reader.try_read_one()
        if parsed is not None:
            obj, consumed = parsed
            if consumed:
                del reader.buf[:consumed]
            return obj
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("Connection closed")
        reader.feed(chunk)


class Connection:
    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.reader = RESPReader()
        self.sock.settimeout(2.0)

    def read_resp(self):
        while True:
            parsed = self.reader.try_read_one()
            if parsed is not None:
                obj, consumed = parsed
                if consumed:
                    del self.reader.buf[:consumed]
                return obj

            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("Connection closed")
            self.reader.feed(chunk)

    def read_rdb(self):
        while b"\r\n" not in self.reader.buf:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("Connection closed")
            self.reader.feed(chunk)

        line, rest_idx = self.reader._read_line(0)
        if not line.startswith(b"$"):
            raise ValueError(f"Expected RDB length prefix '$', got {line}")

        length = int(line[1:])
        del self.reader.buf[:rest_idx]

        while len(self.reader.buf) < length + 2:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("Connection closed")
            self.reader.feed(chunk)

        data = bytes(self.reader.buf[:length])
        if self.reader.buf[length : length + 2] != b"\r\n":
            raise ValueError("Expected CRLF after RDB payload")
        del self.reader.buf[: length + 2]
        return data


class RESPReader:
    def __init__(self) -> None:
        self.buf = bytearray()

    def feed(self, data: bytes) -> None:
        self.buf += data

    def _read_line(self, start: int):
        idx = self.buf.find(CRLF, start)
        if idx == -1:
            return None
        return bytes(self.buf[start:idx]), idx + 2

    def _parse(self, start: int = 0, *, raise_errors: bool = True):
        if start >= len(self.buf):
            return None
        prefix = self.buf[start : start + 1]
        if not prefix:
            return None
        t = prefix[0]

        # Simple String
        if t == ord("+"):
            res = self._read_line(start + 1)
            if res is None:
                return None
            line, end = res
            return line.decode(), end

        # Error
        if t == ord("-"):
            res = self._read_line(start + 1)
            if res is None:
                return None
            line, end = res
            message = line.decode()
            if raise_errors:
                raise RuntimeError(message)
            return RuntimeError(message), end

        # Integer
        if t == ord(":"):
            res = self._read_line(start + 1)
            if res is None:
                return None
            line, end = res
            return int(line), end

        # Bulk String
        if t == ord("$"):
            res = self._read_line(start + 1)
            if res is None:
                return None
            line, end = res
            n = int(line)
            if n == -1:
                return None, end  # represent NULL bulk as None
            need = end + n + 2
            if len(self.buf) < need:
                return None
            data = bytes(self.buf[end : end + n])
            if self.buf[end + n : end + n + 2] != CRLF:
                raise ValueError("Invalid bulk string termination")
            return data.decode(), end + n + 2

        # Array
        if t == ord("*"):
            res = self._read_line(start + 1)
            if res is None:
                return None
            line, end = res
            count = int(line)
            if count == -1:
                return None, end
            items = []
            pos = end
            for _ in range(count):
                parsed = self._parse(pos, raise_errors=False)
                if parsed is None:
                    return None
                item, pos = parsed
                items.append(item)
            return items, pos

        raise ValueError(f"Unknown RESP prefix: {chr(t)!r}")

    def try_read_one(self):
        return self._parse(0)
