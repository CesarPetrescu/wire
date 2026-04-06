//! Wire protocol — binary framing over a single WebSocket.
//!
//! Frame layout (big-endian):
//! ┌──────────┬──────────┬──────────────┬───────────┬──────────┬─────────┐
//! │ magic(2) │ type(1)  │ msg_id(16)   │ flags(1)  │ len(4)   │ payload │
//! └──────────┴──────────┴──────────────┴───────────┴──────────┴─────────┘

use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use thiserror::Error;
use uuid::Uuid;

pub const CHECKSUM_SIZE: usize = 32; // SHA-256 produces 32 bytes

pub const MAGIC: u16 = 0xBE01;
pub const HEADER_SIZE: usize = 2 + 1 + 16 + 1 + 4; // 24 bytes
pub const STREAM_CHUNK_SIZE: usize = 4 * 1024 * 1024; // 4 MiB

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum MessageType {
    Json = 0x01,
    Binary = 0x02,
    File = 0x03,
    Image = 0x04,
    Relay = 0x05,
    HttpRequest = 0x06,
    HttpResponse = 0x07,
    Auth = 0x10,
    AuthOk = 0x11,
    AuthFail = 0x12,
    Ping = 0xFF,
}

impl TryFrom<u8> for MessageType {
    type Error = ProtocolError;
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0x01 => Ok(Self::Json),
            0x02 => Ok(Self::Binary),
            0x03 => Ok(Self::File),
            0x04 => Ok(Self::Image),
            0x05 => Ok(Self::Relay),
            0x06 => Ok(Self::HttpRequest),
            0x07 => Ok(Self::HttpResponse),
            0x10 => Ok(Self::Auth),
            0x11 => Ok(Self::AuthOk),
            0x12 => Ok(Self::AuthFail),
            0xFF => Ok(Self::Ping),
            _ => Err(ProtocolError::BadMessageType(v)),
        }
    }
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct Flags: u8 {
        const NONE         = 0;
        const STREAM_START = 1 << 0;
        const STREAM_CHUNK = 1 << 1;
        const STREAM_END   = 1 << 2;
        const COMPRESSED   = 1 << 3;
    }
}

#[derive(Debug, Clone)]
pub struct FrameHeader {
    pub msg_type: MessageType,
    pub msg_id: [u8; 16],
    pub flags: Flags,
    pub payload_len: u32,
}

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("Frame too short: {0} < {HEADER_SIZE}")]
    FrameTooShort(usize),
    #[error("Bad magic: 0x{0:04X}")]
    BadMagic(u16),
    #[error("Bad message type: 0x{0:02X}")]
    BadMessageType(u8),
    #[error("Payload truncated: got {got}, expected {expected}")]
    PayloadTruncated { got: usize, expected: usize },
    #[error("Decompression error: {0}")]
    DecompressError(String),
    #[error("Compression error: {0}")]
    CompressError(String),
    #[error("Checksum mismatch for file '{filename}': expected {expected}, got {actual}")]
    ChecksumMismatch {
        filename: String,
        expected: String,
        actual: String,
    },
}

/// Encode a single frame with header + payload.
pub fn encode_frame(
    msg_type: MessageType,
    payload: &[u8],
    msg_id: Option<[u8; 16]>,
    flags: Flags,
    compress: bool,
) -> Result<Vec<u8>, ProtocolError> {
    let mid = msg_id.unwrap_or_else(|| *Uuid::new_v4().as_bytes());
    let mut actual_flags = flags;
    let actual_payload;

    if compress && payload.len() > 256 {
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::fast());
        encoder
            .write_all(payload)
            .map_err(|e| ProtocolError::CompressError(e.to_string()))?;
        actual_payload = encoder
            .finish()
            .map_err(|e| ProtocolError::CompressError(e.to_string()))?;
        actual_flags |= Flags::COMPRESSED;
    } else {
        actual_payload = payload.to_vec();
    }

    let mut buf = Vec::with_capacity(HEADER_SIZE + actual_payload.len());
    buf.extend_from_slice(&MAGIC.to_be_bytes());
    buf.push(msg_type as u8);
    buf.extend_from_slice(&mid);
    buf.push(actual_flags.bits());
    buf.extend_from_slice(&(actual_payload.len() as u32).to_be_bytes());
    buf.extend_from_slice(&actual_payload);
    Ok(buf)
}

/// Decode a frame from raw bytes. Returns (header, payload).
pub fn decode_frame(data: &[u8]) -> Result<(FrameHeader, Vec<u8>), ProtocolError> {
    if data.len() < HEADER_SIZE {
        return Err(ProtocolError::FrameTooShort(data.len()));
    }

    let magic = u16::from_be_bytes([data[0], data[1]]);
    if magic != MAGIC {
        return Err(ProtocolError::BadMagic(magic));
    }

    let msg_type = MessageType::try_from(data[2])?;
    let mut msg_id = [0u8; 16];
    msg_id.copy_from_slice(&data[3..19]);
    let flags = Flags::from_bits_truncate(data[19]);
    let payload_len = u32::from_be_bytes([data[20], data[21], data[22], data[23]]) as usize;

    let payload_start = HEADER_SIZE;
    let payload_end = payload_start + payload_len;

    if data.len() < payload_end {
        return Err(ProtocolError::PayloadTruncated {
            got: data.len() - payload_start,
            expected: payload_len,
        });
    }

    let mut payload = data[payload_start..payload_end].to_vec();

    if flags.contains(Flags::COMPRESSED) {
        let mut decoder = ZlibDecoder::new(&payload[..]);
        let mut decompressed = Vec::new();
        decoder
            .read_to_end(&mut decompressed)
            .map_err(|e| ProtocolError::DecompressError(e.to_string()))?;
        payload = decompressed;
    }

    Ok((
        FrameHeader {
            msg_type,
            msg_id,
            flags,
            payload_len: payload.len() as u32,
        },
        payload,
    ))
}

/// Encode a file payload: 2-byte filename length + filename + SHA-256 checksum + data.
pub fn encode_file_payload(filename: &str, data: &[u8]) -> Vec<u8> {
    let name_bytes = filename.as_bytes();
    let checksum = Sha256::digest(data);
    let mut buf = Vec::with_capacity(2 + name_bytes.len() + CHECKSUM_SIZE + data.len());
    buf.extend_from_slice(&(name_bytes.len() as u16).to_be_bytes());
    buf.extend_from_slice(name_bytes);
    buf.extend_from_slice(&checksum);
    buf.extend_from_slice(data);
    buf
}

/// Decode a file payload back to (filename, data) and verify SHA-256 checksum.
pub fn decode_file_payload(payload: &[u8]) -> Result<(String, Vec<u8>), ProtocolError> {
    if payload.len() < 2 {
        return Err(ProtocolError::FrameTooShort(payload.len()));
    }
    let name_len = u16::from_be_bytes([payload[0], payload[1]]) as usize;
    let filename = String::from_utf8_lossy(&payload[2..2 + name_len]).to_string();
    let checksum_start = 2 + name_len;
    let expected_checksum = &payload[checksum_start..checksum_start + CHECKSUM_SIZE];
    let data = payload[checksum_start + CHECKSUM_SIZE..].to_vec();
    let actual_checksum = Sha256::digest(&data);
    if actual_checksum.as_slice() != expected_checksum {
        return Err(ProtocolError::ChecksumMismatch {
            filename,
            expected: hex::encode(expected_checksum),
            actual: hex::encode(actual_checksum),
        });
    }
    Ok((filename, data))
}

/// Encode a relay payload for peer-to-peer messaging via the Controller.
pub fn encode_relay_payload(
    source_fp: &str,
    dest_fp: &str,
    inner_msg_type: MessageType,
    inner_payload: &[u8],
) -> Vec<u8> {
    let src = source_fp.as_bytes();
    let dst = dest_fp.as_bytes();
    let mut buf = Vec::with_capacity(2 + src.len() + 2 + dst.len() + 1 + inner_payload.len());
    buf.extend_from_slice(&(src.len() as u16).to_be_bytes());
    buf.extend_from_slice(src);
    buf.extend_from_slice(&(dst.len() as u16).to_be_bytes());
    buf.extend_from_slice(dst);
    buf.push(inner_msg_type as u8);
    buf.extend_from_slice(inner_payload);
    buf
}

/// Decode a relay payload. Returns (source_fp, dest_fp, inner_msg_type, inner_payload).
pub fn decode_relay_payload(
    payload: &[u8],
) -> Result<(String, String, MessageType, Vec<u8>), ProtocolError> {
    let mut offset = 0;
    let src_len = u16::from_be_bytes([payload[offset], payload[offset + 1]]) as usize;
    offset += 2;
    let source_fp = String::from_utf8_lossy(&payload[offset..offset + src_len]).to_string();
    offset += src_len;
    let dst_len = u16::from_be_bytes([payload[offset], payload[offset + 1]]) as usize;
    offset += 2;
    let dest_fp = String::from_utf8_lossy(&payload[offset..offset + dst_len]).to_string();
    offset += dst_len;
    let inner_msg_type = MessageType::try_from(payload[offset])?;
    offset += 1;
    let inner_payload = payload[offset..].to_vec();
    Ok((source_fp, dest_fp, inner_msg_type, inner_payload))
}

// ── HTTP method enum ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum HttpMethod {
    Get = 0,
    Post = 1,
    Put = 2,
    Delete = 3,
    Patch = 4,
    Head = 5,
    Options = 6,
}

impl TryFrom<u8> for HttpMethod {
    type Error = ProtocolError;
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::Get),
            1 => Ok(Self::Post),
            2 => Ok(Self::Put),
            3 => Ok(Self::Delete),
            4 => Ok(Self::Patch),
            5 => Ok(Self::Head),
            6 => Ok(Self::Options),
            _ => Err(ProtocolError::BadMessageType(v)),
        }
    }
}

impl HttpMethod {
    pub fn from_str(s: &str) -> Result<Self, ProtocolError> {
        match s.to_uppercase().as_str() {
            "GET" => Ok(Self::Get),
            "POST" => Ok(Self::Post),
            "PUT" => Ok(Self::Put),
            "DELETE" => Ok(Self::Delete),
            "PATCH" => Ok(Self::Patch),
            "HEAD" => Ok(Self::Head),
            "OPTIONS" => Ok(Self::Options),
            _ => Err(ProtocolError::BadMessageType(0xFF)),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Delete => "DELETE",
            Self::Patch => "PATCH",
            Self::Head => "HEAD",
            Self::Options => "OPTIONS",
        }
    }
}

// ── HTTP tunnel payloads ─────────────────────────────────────────────────────

/// Encode an HTTP request for tunneling through WebSocket.
pub fn encode_http_request(
    method: HttpMethod,
    path: &str,
    query: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Vec<u8> {
    let path_b = path.as_bytes();
    let query_b = query.as_bytes();
    let mut buf = Vec::new();
    buf.push(method as u8);
    buf.extend_from_slice(&(path_b.len() as u16).to_be_bytes());
    buf.extend_from_slice(path_b);
    buf.extend_from_slice(&(query_b.len() as u16).to_be_bytes());
    buf.extend_from_slice(query_b);
    buf.extend_from_slice(&(headers.len() as u16).to_be_bytes());
    for (key, val) in headers {
        let kb = key.as_bytes();
        let vb = val.as_bytes();
        buf.extend_from_slice(&(kb.len() as u16).to_be_bytes());
        buf.extend_from_slice(kb);
        buf.extend_from_slice(&(vb.len() as u16).to_be_bytes());
        buf.extend_from_slice(vb);
    }
    buf.extend_from_slice(&(body.len() as u32).to_be_bytes());
    buf.extend_from_slice(body);
    buf
}

/// Decode an HTTP request payload. Returns (method, path, query, headers, body).
pub fn decode_http_request(
    payload: &[u8],
) -> Result<(HttpMethod, String, String, Vec<(String, String)>, Vec<u8>), ProtocolError> {
    let mut offset = 0;
    let method = HttpMethod::try_from(payload[offset])?;
    offset += 1;
    let path_len = u16::from_be_bytes([payload[offset], payload[offset + 1]]) as usize;
    offset += 2;
    let path = String::from_utf8_lossy(&payload[offset..offset + path_len]).to_string();
    offset += path_len;
    let query_len = u16::from_be_bytes([payload[offset], payload[offset + 1]]) as usize;
    offset += 2;
    let query = String::from_utf8_lossy(&payload[offset..offset + query_len]).to_string();
    offset += query_len;
    let header_count = u16::from_be_bytes([payload[offset], payload[offset + 1]]) as usize;
    offset += 2;
    let mut headers = Vec::with_capacity(header_count);
    for _ in 0..header_count {
        let kl = u16::from_be_bytes([payload[offset], payload[offset + 1]]) as usize;
        offset += 2;
        let key = String::from_utf8_lossy(&payload[offset..offset + kl]).to_string();
        offset += kl;
        let vl = u16::from_be_bytes([payload[offset], payload[offset + 1]]) as usize;
        offset += 2;
        let val = String::from_utf8_lossy(&payload[offset..offset + vl]).to_string();
        offset += vl;
        headers.push((key, val));
    }
    let body_len = u32::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
    ]) as usize;
    offset += 4;
    let body = payload[offset..offset + body_len].to_vec();
    Ok((method, path, query, headers, body))
}

/// Encode an HTTP response for tunneling through WebSocket.
pub fn encode_http_response(
    status_code: u16,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&status_code.to_be_bytes());
    buf.extend_from_slice(&(headers.len() as u16).to_be_bytes());
    for (key, val) in headers {
        let kb = key.as_bytes();
        let vb = val.as_bytes();
        buf.extend_from_slice(&(kb.len() as u16).to_be_bytes());
        buf.extend_from_slice(kb);
        buf.extend_from_slice(&(vb.len() as u16).to_be_bytes());
        buf.extend_from_slice(vb);
    }
    buf.extend_from_slice(&(body.len() as u32).to_be_bytes());
    buf.extend_from_slice(body);
    buf
}

/// Decode an HTTP response payload. Returns (status_code, headers, body).
pub fn decode_http_response(
    payload: &[u8],
) -> Result<(u16, Vec<(String, String)>, Vec<u8>), ProtocolError> {
    let mut offset = 0;
    let status_code = u16::from_be_bytes([payload[offset], payload[offset + 1]]);
    offset += 2;
    let header_count = u16::from_be_bytes([payload[offset], payload[offset + 1]]) as usize;
    offset += 2;
    let mut headers = Vec::with_capacity(header_count);
    for _ in 0..header_count {
        let kl = u16::from_be_bytes([payload[offset], payload[offset + 1]]) as usize;
        offset += 2;
        let key = String::from_utf8_lossy(&payload[offset..offset + kl]).to_string();
        offset += kl;
        let vl = u16::from_be_bytes([payload[offset], payload[offset + 1]]) as usize;
        offset += 2;
        let val = String::from_utf8_lossy(&payload[offset..offset + vl]).to_string();
        offset += vl;
        headers.push((key, val));
    }
    let body_len = u32::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
    ]) as usize;
    offset += 4;
    let body = payload[offset..offset + body_len].to_vec();
    Ok((status_code, headers, body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_roundtrip() {
        let payload = b"{\"hello\":\"world\"}";
        let frame = encode_frame(MessageType::Json, payload, None, Flags::NONE, false).unwrap();
        let (header, decoded) = decode_frame(&frame).unwrap();
        assert_eq!(header.msg_type, MessageType::Json);
        assert_eq!(decoded, payload);
    }

    #[test]
    fn test_binary_roundtrip() {
        let payload: Vec<u8> = (0..=255).collect::<Vec<u8>>().repeat(10);
        let frame =
            encode_frame(MessageType::Binary, &payload, None, Flags::NONE, false).unwrap();
        let (header, decoded) = decode_frame(&frame).unwrap();
        assert_eq!(header.msg_type, MessageType::Binary);
        assert_eq!(decoded, payload);
    }

    #[test]
    fn test_compressed_roundtrip() {
        let payload = vec![b'A'; 10000];
        let frame =
            encode_frame(MessageType::Binary, &payload, None, Flags::NONE, true).unwrap();
        assert!(frame.len() < payload.len());
        let (header, decoded) = decode_frame(&frame).unwrap();
        assert!(header.flags.contains(Flags::COMPRESSED));
        assert_eq!(decoded, payload);
    }

    #[test]
    fn test_small_not_compressed() {
        let payload = b"tiny";
        let frame = encode_frame(MessageType::Json, payload, None, Flags::NONE, true).unwrap();
        let (header, decoded) = decode_frame(&frame).unwrap();
        assert!(!header.flags.contains(Flags::COMPRESSED));
        assert_eq!(decoded, payload);
    }

    #[test]
    fn test_custom_msg_id() {
        let mid = *Uuid::new_v4().as_bytes();
        let frame = encode_frame(MessageType::Json, b"{}", Some(mid), Flags::NONE, false).unwrap();
        let (header, _) = decode_frame(&frame).unwrap();
        assert_eq!(header.msg_id, mid);
    }

    #[test]
    fn test_stream_flags() {
        for flag in [Flags::STREAM_START, Flags::STREAM_CHUNK, Flags::STREAM_END] {
            let frame = encode_frame(MessageType::File, b"data", None, flag, false).unwrap();
            let (header, _) = decode_frame(&frame).unwrap();
            assert!(header.flags.contains(flag));
        }
    }

    #[test]
    fn test_bad_magic() {
        let frame = encode_frame(MessageType::Json, b"{}", None, Flags::NONE, false).unwrap();
        let mut bad = frame.clone();
        bad[0] = 0x00;
        bad[1] = 0x00;
        assert!(matches!(decode_frame(&bad), Err(ProtocolError::BadMagic(0))));
    }

    #[test]
    fn test_truncated_frame() {
        assert!(matches!(
            decode_frame(&[0u8; 10]),
            Err(ProtocolError::FrameTooShort(10))
        ));
        assert!(matches!(
            decode_frame(&[]),
            Err(ProtocolError::FrameTooShort(0))
        ));
    }

    #[test]
    fn test_file_payload_roundtrip() {
        let filename = "test.zip";
        let data = vec![0x50, 0x4b, 0x03, 0x04, 0x00, 0x00];
        let encoded = encode_file_payload(filename, &data);
        let (dec_name, dec_data) = decode_file_payload(&encoded).unwrap();
        assert_eq!(dec_name, filename);
        assert_eq!(dec_data, data);
    }

    #[test]
    fn test_empty_payload() {
        let frame = encode_frame(MessageType::Ping, b"", None, Flags::NONE, false).unwrap();
        let (header, payload) = decode_frame(&frame).unwrap();
        assert_eq!(header.msg_type, MessageType::Ping);
        assert!(payload.is_empty());
    }

    #[test]
    fn test_checksum_embedded_in_payload() {
        let filename = "test.zip";
        let data = vec![0x50, 0x4b, 0x03, 0x04, 0x00, 0x00];
        let encoded = encode_file_payload(filename, &data);
        let checksum_offset = 2 + filename.len();
        let embedded = &encoded[checksum_offset..checksum_offset + CHECKSUM_SIZE];
        let expected = Sha256::digest(&data);
        assert_eq!(embedded, expected.as_slice());
    }

    #[test]
    fn test_checksum_mismatch_raises() {
        let filename = "corrupted.zip";
        let data = b"original data content";
        let mut encoded = encode_file_payload(filename, data);
        // Corrupt a byte in the file data (last byte)
        let last = encoded.len() - 1;
        encoded[last] ^= 0xFF;
        let result = decode_file_payload(&encoded);
        assert!(matches!(result, Err(ProtocolError::ChecksumMismatch { .. })));
    }

    #[test]
    fn test_checksum_valid_empty_data() {
        let filename = "empty.bin";
        let data: &[u8] = b"";
        let encoded = encode_file_payload(filename, data);
        let (dec_name, dec_data) = decode_file_payload(&encoded).unwrap();
        assert_eq!(dec_name, filename);
        assert!(dec_data.is_empty());
    }
}
