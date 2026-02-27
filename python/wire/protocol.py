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
                      filename UTF-8, then file bytes)
  0x04  IMAGE       - image data (same sub-header as FILE)
  0x10  AUTH        - authentication handshake
  0x11  AUTH_OK     - auth accepted
  0x12  AUTH_FAIL   - auth rejected
  0xFF  PING/PONG   - keepalive
"""

import enum
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


def encode_file_payload(filename: str, data: bytes) -> bytes:
    """Encode a file payload: 2-byte filename length + filename + data."""
    name_bytes = filename.encode("utf-8")
    return struct.pack("!H", len(name_bytes)) + name_bytes + data


def decode_file_payload(payload: bytes) -> tuple[str, bytes]:
    """Decode a file payload back to (filename, data)."""
    name_len = struct.unpack("!H", payload[:2])[0]
    filename = payload[2 : 2 + name_len].decode("utf-8")
    data = payload[2 + name_len :]
    return filename, data
