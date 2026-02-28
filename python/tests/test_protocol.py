"""Unit tests for the wire protocol framing layer."""

import json
import uuid
import zlib

import pytest

from wire.protocol import (
    CHECKSUM_SIZE,
    HEADER_SIZE,
    ChecksumError,
    Flags,
    MessageType,
    decode_file_payload,
    decode_frame,
    encode_file_payload,
    encode_frame,
)


class TestFrameEncoding:
    def test_json_roundtrip(self):
        data = {"hello": "world", "count": 42}
        payload = json.dumps(data).encode()
        frame = encode_frame(MessageType.JSON, payload)
        header, decoded = decode_frame(frame)
        assert header.msg_type == MessageType.JSON
        assert json.loads(decoded) == data

    def test_binary_roundtrip(self):
        payload = bytes(range(256)) * 10
        frame = encode_frame(MessageType.BINARY, payload)
        header, decoded = decode_frame(frame)
        assert header.msg_type == MessageType.BINARY
        assert decoded == payload

    def test_compressed_roundtrip(self):
        # Repetitive data compresses well
        payload = b"A" * 10000
        frame = encode_frame(MessageType.BINARY, payload, compress=True)
        assert len(frame) < len(payload)  # should be smaller
        header, decoded = decode_frame(frame)
        assert decoded == payload
        assert header.flags & Flags.COMPRESSED

    def test_small_payload_not_compressed(self):
        """Payloads <= 256 bytes should not be compressed even if requested."""
        payload = b"tiny"
        frame = encode_frame(MessageType.JSON, payload, compress=True)
        header, decoded = decode_frame(frame)
        assert not (header.flags & Flags.COMPRESSED)
        assert decoded == payload

    def test_custom_msg_id(self):
        msg_id = uuid.uuid4().bytes
        frame = encode_frame(MessageType.JSON, b"{}", msg_id=msg_id)
        header, _ = decode_frame(frame)
        assert header.msg_id == msg_id

    def test_stream_flags_preserved(self):
        for flag in [Flags.STREAM_START, Flags.STREAM_CHUNK, Flags.STREAM_END]:
            frame = encode_frame(MessageType.FILE, b"data", flags=flag)
            header, _ = decode_frame(frame)
            assert header.flags & flag

    def test_bad_magic_raises(self):
        frame = encode_frame(MessageType.JSON, b"{}")
        bad = b"\x00\x00" + frame[2:]
        with pytest.raises(ValueError, match="Bad magic"):
            decode_frame(bad)

    def test_truncated_frame_raises(self):
        with pytest.raises(ValueError, match="Frame too short"):
            decode_frame(b"\x00" * 10)

    def test_truncated_payload_raises(self):
        frame = encode_frame(MessageType.JSON, b"hello world")
        with pytest.raises(ValueError, match="Payload truncated"):
            decode_frame(frame[:-5])

    def test_empty_payload(self):
        frame = encode_frame(MessageType.PING, b"")
        header, payload = decode_frame(frame)
        assert header.msg_type == MessageType.PING
        assert payload == b""


class TestFilePayload:
    def test_roundtrip(self):
        filename = "test_archive.zip"
        data = b"\x50\x4b\x03\x04" + b"\x00" * 100  # fake zip header
        encoded = encode_file_payload(filename, data)
        dec_name, dec_data = decode_file_payload(encoded)
        assert dec_name == filename
        assert dec_data == data

    def test_unicode_filename(self):
        filename = "datos_año_2024.zip"
        data = b"content"
        encoded = encode_file_payload(filename, data)
        dec_name, dec_data = decode_file_payload(encoded)
        assert dec_name == filename

    def test_large_filename(self):
        filename = "a" * 1000 + ".bin"
        data = b"x"
        encoded = encode_file_payload(filename, data)
        dec_name, dec_data = decode_file_payload(encoded)
        assert dec_name == filename
        assert dec_data == data

    def test_checksum_embedded_in_payload(self):
        """Encoded payload should contain the SHA-256 checksum."""
        import hashlib

        filename = "test.zip"
        data = b"\x50\x4b\x03\x04" + b"\x00" * 100
        encoded = encode_file_payload(filename, data)
        name_bytes = filename.encode("utf-8")
        checksum_offset = 2 + len(name_bytes)
        embedded = encoded[checksum_offset : checksum_offset + CHECKSUM_SIZE]
        assert embedded == hashlib.sha256(data).digest()

    def test_checksum_mismatch_raises(self):
        """Corrupted file data should raise ChecksumError on decode."""
        filename = "corrupted.zip"
        data = b"original data content"
        encoded = bytearray(encode_file_payload(filename, data))
        # Corrupt a byte in the file data (after filename + checksum)
        encoded[-1] ^= 0xFF
        with pytest.raises(ChecksumError, match="checksum mismatch"):
            decode_file_payload(bytes(encoded))

    def test_checksum_valid_empty_data(self):
        """Checksum should work correctly for empty file data."""
        filename = "empty.bin"
        data = b""
        encoded = encode_file_payload(filename, data)
        dec_name, dec_data = decode_file_payload(encoded)
        assert dec_name == filename
        assert dec_data == data
