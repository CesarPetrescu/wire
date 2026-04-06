"""
Wire protocol - binary framing over a single WebSocket.

Frame layout (all multi-byte integers are big-endian):
┌──────────┬──────────┬──────────────┬───────────┬──────────┬─────────┐
│ magic(2) │ type(1)  │ msg_id(16)   │ flags(1)  │ len(4)   │ payload │
└──────────┴──────────┴──────────────┴───────────┴──────────┴─────────┘
  0xW1RE      see below  UUID bytes    bit flags   payload sz  raw bytes

Flags:
  bit 0: STREAM_START   - first chunk of a streaming transfer
  bit 1: STREAM_CHUNK   - continuation chunk
  bit 2: STREAM_END     - final chunk (may also carry data)
  bit 3: COMPRESSED     - payload is zlib-compressed

Message types:
  0x01  JSON        - payload is UTF-8 JSON
  0x02  BINARY      - arbitrary binary blob
  0x03  FILE        - file transfer (first 2 bytes = filename length, then
                      filename UTF-8, then 32-byte SHA-256 checksum, then file bytes)
  0x04  IMAGE       - image data (same sub-header as FILE)
  0x05  RELAY       - peer-to-peer relay via Controller
                      (2-byte src_fp len + src_fp + 2-byte dst_fp len + dst_fp
                       + 1-byte inner msg type + inner payload)
  0x06  HTTP_REQUEST  - HTTP request tunneled through WebSocket
  0x07  HTTP_RESPONSE - HTTP response tunneled through WebSocket
  0x10  AUTH        - authentication handshake
  0x11  AUTH_OK     - auth accepted
  0x12  AUTH_FAIL   - auth rejected
  0xFF  PING/PONG   - keepalive
"""

import enum
import hashlib
import struct
import uuid
import zlib
from dataclasses import dataclass, field
from typing import Optional

MAGIC = struct.pack("!H", 0xBE01)  # our magic number
HEADER_SIZE = 2 + 1 + 16 + 1 + 4  # 24 bytes

STREAM_CHUNK_SIZE = 4 * 1024 * 1024  # 4 MiB per chunk


class MessageType(enum.IntEnum):
    JSON = 0x01
    BINARY = 0x02
    FILE = 0x03
    IMAGE = 0x04
    RELAY = 0x05
    HTTP_REQUEST = 0x06
    HTTP_RESPONSE = 0x07
    AUTH = 0x10
    AUTH_OK = 0x11
    AUTH_FAIL = 0x12
    PING = 0xFF


class Flags(enum.IntFlag):
    NONE = 0
    STREAM_START = 1 << 0
    STREAM_CHUNK = 1 << 1
    STREAM_END = 1 << 2
    COMPRESSED = 1 << 3


@dataclass
class FrameHeader:
    msg_type: MessageType
    msg_id: bytes = field(default_factory=lambda: uuid.uuid4().bytes)
    flags: Flags = Flags.NONE
    payload_len: int = 0


def encode_frame(
    msg_type: MessageType,
    payload: bytes,
    msg_id: Optional[bytes] = None,
    flags: Flags = Flags.NONE,
    compress: bool = False,
) -> bytes:
    """Encode a single frame with header + payload."""
    if msg_id is None:
        msg_id = uuid.uuid4().bytes
    if compress and len(payload) > 256:
        payload = zlib.compress(payload, level=6)
        flags |= Flags.COMPRESSED

    header = struct.pack(
        "!H B 16s B I",
        0xBE01,
        int(msg_type),
        msg_id,
        int(flags),
        len(payload),
    )
    return header + payload


def decode_frame(data: bytes) -> tuple[FrameHeader, bytes]:
    """Decode a frame from raw bytes. Returns (header, payload)."""
    if len(data) < HEADER_SIZE:
        raise ValueError(f"Frame too short: {len(data)} < {HEADER_SIZE}")

    magic, msg_type_raw, msg_id, flags_raw, payload_len = struct.unpack(
        "!H B 16s B I", data[:HEADER_SIZE]
    )
    if magic != 0xBE01:
        raise ValueError(f"Bad magic: 0x{magic:04X}")

    payload = data[HEADER_SIZE : HEADER_SIZE + payload_len]
    if len(payload) != payload_len:
        raise ValueError(
            f"Payload truncated: got {len(payload)}, expected {payload_len}"
        )

    flags = Flags(flags_raw)
    if flags & Flags.COMPRESSED:
        payload = zlib.decompress(payload)

    header = FrameHeader(
        msg_type=MessageType(msg_type_raw),
        msg_id=msg_id,
        flags=flags,
        payload_len=len(payload),
    )
    return header, payload


CHECKSUM_SIZE = 32  # SHA-256 produces 32 bytes


class ChecksumError(Exception):
    """Raised when a file payload checksum does not match."""


def encode_file_payload(filename: str, data: bytes) -> bytes:
    """Encode a file payload: 2-byte filename length + filename + SHA-256 checksum + data."""
    name_bytes = filename.encode("utf-8")
    checksum = hashlib.sha256(data).digest()
    return struct.pack("!H", len(name_bytes)) + name_bytes + checksum + data


def decode_file_payload(payload: bytes) -> tuple[str, bytes]:
    """Decode a file payload back to (filename, data) and verify SHA-256 checksum."""
    name_len = struct.unpack("!H", payload[:2])[0]
    filename = payload[2 : 2 + name_len].decode("utf-8")
    checksum_start = 2 + name_len
    expected_checksum = payload[checksum_start : checksum_start + CHECKSUM_SIZE]
    data = payload[checksum_start + CHECKSUM_SIZE :]
    actual_checksum = hashlib.sha256(data).digest()
    if actual_checksum != expected_checksum:
        raise ChecksumError(
            f"File '{filename}' checksum mismatch: "
            f"expected {expected_checksum.hex()}, got {actual_checksum.hex()}"
        )
    return filename, data


def encode_relay_payload(
    source_fp: str, dest_fp: str, inner_msg_type: MessageType, inner_payload: bytes
) -> bytes:
    """Encode a relay payload for peer-to-peer messaging via the Controller."""
    src = source_fp.encode("utf-8")
    dst = dest_fp.encode("utf-8")
    return (
        struct.pack("!H", len(src))
        + src
        + struct.pack("!H", len(dst))
        + dst
        + struct.pack("!B", int(inner_msg_type))
        + inner_payload
    )


def decode_relay_payload(
    payload: bytes,
) -> tuple[str, str, MessageType, bytes]:
    """Decode a relay payload. Returns (source_fp, dest_fp, inner_msg_type, inner_payload)."""
    offset = 0
    src_len = struct.unpack("!H", payload[offset : offset + 2])[0]
    offset += 2
    source_fp = payload[offset : offset + src_len].decode("utf-8")
    offset += src_len
    dst_len = struct.unpack("!H", payload[offset : offset + 2])[0]
    offset += 2
    dest_fp = payload[offset : offset + dst_len].decode("utf-8")
    offset += dst_len
    inner_msg_type = MessageType(payload[offset])
    offset += 1
    inner_payload = payload[offset:]
    return source_fp, dest_fp, inner_msg_type, inner_payload


# ── HTTP method enum ─────────────────────────────────────────────────────────

class HttpMethod(enum.IntEnum):
    GET = 0
    POST = 1
    PUT = 2
    DELETE = 3
    PATCH = 4
    HEAD = 5
    OPTIONS = 6

    @classmethod
    def from_str(cls, method: str) -> "HttpMethod":
        return cls[method.upper()]

    def to_str(self) -> str:
        return self.name


# ── HTTP tunnel payloads ─────────────────────────────────────────────────────

def encode_http_request(
    method: HttpMethod,
    path: str,
    query: str,
    headers: list[tuple[str, str]],
    body: bytes,
) -> bytes:
    """Encode an HTTP request for tunneling through WebSocket.

    Format:
      [1-byte method][2-byte path len][path][2-byte query len][query]
      [2-byte header count]([2-byte key len][key][2-byte val len][val])*
      [4-byte body len][body]
    """
    path_b = path.encode("utf-8")
    query_b = query.encode("utf-8")
    parts = [
        struct.pack("!B", int(method)),
        struct.pack("!H", len(path_b)),
        path_b,
        struct.pack("!H", len(query_b)),
        query_b,
        struct.pack("!H", len(headers)),
    ]
    for key, val in headers:
        kb = key.encode("utf-8")
        vb = val.encode("utf-8")
        parts.append(struct.pack("!H", len(kb)))
        parts.append(kb)
        parts.append(struct.pack("!H", len(vb)))
        parts.append(vb)
    parts.append(struct.pack("!I", len(body)))
    parts.append(body)
    return b"".join(parts)


def decode_http_request(
    payload: bytes,
) -> tuple[HttpMethod, str, str, list[tuple[str, str]], bytes]:
    """Decode an HTTP request payload. Returns (method, path, query, headers, body)."""
    offset = 0
    method = HttpMethod(payload[offset])
    offset += 1
    path_len = struct.unpack("!H", payload[offset : offset + 2])[0]
    offset += 2
    path = payload[offset : offset + path_len].decode("utf-8")
    offset += path_len
    query_len = struct.unpack("!H", payload[offset : offset + 2])[0]
    offset += 2
    query = payload[offset : offset + query_len].decode("utf-8")
    offset += query_len
    header_count = struct.unpack("!H", payload[offset : offset + 2])[0]
    offset += 2
    headers: list[tuple[str, str]] = []
    for _ in range(header_count):
        kl = struct.unpack("!H", payload[offset : offset + 2])[0]
        offset += 2
        key = payload[offset : offset + kl].decode("utf-8")
        offset += kl
        vl = struct.unpack("!H", payload[offset : offset + 2])[0]
        offset += 2
        val = payload[offset : offset + vl].decode("utf-8")
        offset += vl
        headers.append((key, val))
    body_len = struct.unpack("!I", payload[offset : offset + 4])[0]
    offset += 4
    body = payload[offset : offset + body_len]
    return method, path, query, headers, body


def encode_http_response(
    status_code: int,
    headers: list[tuple[str, str]],
    body: bytes,
) -> bytes:
    """Encode an HTTP response for tunneling through WebSocket.

    Format:
      [2-byte status][2-byte header count]
      ([2-byte key len][key][2-byte val len][val])*
      [4-byte body len][body]
    """
    parts = [
        struct.pack("!H", status_code),
        struct.pack("!H", len(headers)),
    ]
    for key, val in headers:
        kb = key.encode("utf-8")
        vb = val.encode("utf-8")
        parts.append(struct.pack("!H", len(kb)))
        parts.append(kb)
        parts.append(struct.pack("!H", len(vb)))
        parts.append(vb)
    parts.append(struct.pack("!I", len(body)))
    parts.append(body)
    return b"".join(parts)


def decode_http_response(
    payload: bytes,
) -> tuple[int, list[tuple[str, str]], bytes]:
    """Decode an HTTP response payload. Returns (status_code, headers, body)."""
    offset = 0
    status_code = struct.unpack("!H", payload[offset : offset + 2])[0]
    offset += 2
    header_count = struct.unpack("!H", payload[offset : offset + 2])[0]
    offset += 2
    headers: list[tuple[str, str]] = []
    for _ in range(header_count):
        kl = struct.unpack("!H", payload[offset : offset + 2])[0]
        offset += 2
        key = payload[offset : offset + kl].decode("utf-8")
        offset += kl
        vl = struct.unpack("!H", payload[offset : offset + 2])[0]
        offset += 2
        val = payload[offset : offset + vl].decode("utf-8")
        offset += vl
        headers.append((key, val))
    body_len = struct.unpack("!I", payload[offset : offset + 4])[0]
    offset += 4
    body = payload[offset : offset + body_len]
    return status_code, headers, body
