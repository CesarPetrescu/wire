use wire_rs::protocol::*;

// ── Protocol frame tests ────────────────────────────────────────────────────

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
    let payload: Vec<u8> = (0..=255u8).collect::<Vec<u8>>().repeat(10); // 2560 bytes
    assert_eq!(payload.len(), 2560);
    let frame = encode_frame(MessageType::Binary, &payload, None, Flags::NONE, false).unwrap();
    let (header, decoded) = decode_frame(&frame).unwrap();
    assert_eq!(header.msg_type, MessageType::Binary);
    assert_eq!(decoded, payload);
}

#[test]
fn test_compressed_roundtrip() {
    let payload = vec![0x41u8; 10_000]; // 10KB of 'A'
    let frame = encode_frame(MessageType::Binary, &payload, None, Flags::NONE, true).unwrap();
    // Compressed frame must be smaller than uncompressed payload + header
    assert!(frame.len() < payload.len());
    let (header, decoded) = decode_frame(&frame).unwrap();
    assert!(header.flags.contains(Flags::COMPRESSED));
    assert_eq!(decoded, payload);
}

#[test]
fn test_small_payload_not_compressed() {
    let payload = b"tiny";
    let frame = encode_frame(MessageType::Json, payload, None, Flags::NONE, true).unwrap();
    let (header, decoded) = decode_frame(&frame).unwrap();
    assert!(!header.flags.contains(Flags::COMPRESSED));
    assert_eq!(decoded, payload.as_slice());
}

#[test]
fn test_custom_msg_id() {
    let mid = uuid::Uuid::new_v4().into_bytes();
    let frame = encode_frame(MessageType::Json, b"{}", Some(mid), Flags::NONE, false).unwrap();
    let (header, _) = decode_frame(&frame).unwrap();
    assert_eq!(header.msg_id, mid);
}

#[test]
fn test_stream_flags_preserved() {
    for flag in [Flags::STREAM_START, Flags::STREAM_CHUNK, Flags::STREAM_END] {
        let frame = encode_frame(MessageType::File, b"data", None, flag, false).unwrap();
        let (header, _) = decode_frame(&frame).unwrap();
        assert!(header.flags.contains(flag), "flag {:?} not preserved", flag);
    }
}

#[test]
fn test_bad_magic_raises() {
    let frame = encode_frame(MessageType::Json, b"{}", None, Flags::NONE, false).unwrap();
    let mut bad = frame.clone();
    bad[0] = 0x00;
    bad[1] = 0x00;
    match decode_frame(&bad) {
        Err(ProtocolError::BadMagic(_)) => {}
        other => panic!("expected BadMagic, got {:?}", other),
    }
}

#[test]
fn test_truncated_frame_raises() {
    match decode_frame(&[0u8; 10]) {
        Err(ProtocolError::FrameTooShort(10)) => {}
        other => panic!("expected FrameTooShort(10), got {:?}", other),
    }
}

#[test]
fn test_truncated_payload_raises() {
    // Build a valid header but claim payload is larger than what follows
    let mut frame = encode_frame(MessageType::Json, b"hello", None, Flags::NONE, false).unwrap();
    // Overwrite payload_len (bytes 20..24) to something larger
    let big_len: u32 = 9999;
    frame[20..24].copy_from_slice(&big_len.to_be_bytes());
    match decode_frame(&frame) {
        Err(ProtocolError::PayloadTruncated { .. }) => {}
        other => panic!("expected PayloadTruncated, got {:?}", other),
    }
}

#[test]
fn test_empty_payload() {
    let frame = encode_frame(MessageType::Ping, b"", None, Flags::NONE, false).unwrap();
    let (header, payload) = decode_frame(&frame).unwrap();
    assert_eq!(header.msg_type, MessageType::Ping);
    assert!(payload.is_empty());
}

// ── File payload tests ──────────────────────────────────────────────────────

#[test]
fn test_file_roundtrip() {
    let filename = "test.zip";
    let data = vec![0x50, 0x4b, 0x03, 0x04, 0x00, 0x00];
    let encoded = encode_file_payload(filename, &data);
    let (dec_name, dec_data) = decode_file_payload(&encoded).unwrap();
    assert_eq!(dec_name, filename);
    assert_eq!(dec_data, data);
}

#[test]
fn test_file_unicode_filename() {
    let filename = "日本語ファイル名.txt";
    let data = b"unicode content";
    let encoded = encode_file_payload(filename, data);
    let (dec_name, dec_data) = decode_file_payload(&encoded).unwrap();
    assert_eq!(dec_name, filename);
    assert_eq!(dec_data, data);
}

#[test]
fn test_file_large_filename() {
    let filename: String = "x".repeat(200);
    let data = b"data";
    let encoded = encode_file_payload(&filename, data);
    let (dec_name, dec_data) = decode_file_payload(&encoded).unwrap();
    assert_eq!(dec_name, filename);
    assert_eq!(dec_data, data);
}

#[test]
fn test_file_checksum_mismatch() {
    let filename = "corrupted.bin";
    let data = b"original data content";
    let mut encoded = encode_file_payload(filename, data);
    // Corrupt last byte of the file data
    let last = encoded.len() - 1;
    encoded[last] ^= 0xFF;
    match decode_file_payload(&encoded) {
        Err(ProtocolError::ChecksumMismatch { .. }) => {}
        other => panic!("expected ChecksumMismatch, got {:?}", other),
    }
}

#[test]
fn test_file_empty_data() {
    let filename = "empty.bin";
    let data: &[u8] = b"";
    let encoded = encode_file_payload(filename, data);
    let (dec_name, dec_data) = decode_file_payload(&encoded).unwrap();
    assert_eq!(dec_name, filename);
    assert!(dec_data.is_empty());
}

#[test]
fn test_file_checksum_embedded() {
    use sha2::{Digest, Sha256};
    let filename = "test.zip";
    let data = vec![0x50, 0x4b, 0x03, 0x04, 0x00, 0x00];
    let encoded = encode_file_payload(filename, &data);
    let checksum_offset = 2 + filename.len();
    let embedded = &encoded[checksum_offset..checksum_offset + 32];
    let expected = Sha256::digest(&data);
    assert_eq!(embedded, expected.as_slice());
}

// ── Relay payload tests ─────────────────────────────────────────────────────

#[test]
fn test_relay_roundtrip() {
    let source_fp = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
    let dest_fp = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
    let inner_payload = b"{\"action\":\"hello\"}";
    let encoded = encode_relay_payload(source_fp, dest_fp, MessageType::Json, inner_payload);
    let (dec_src, dec_dst, dec_type, dec_payload) = decode_relay_payload(&encoded).unwrap();
    assert_eq!(dec_src, source_fp);
    assert_eq!(dec_dst, dest_fp);
    assert_eq!(dec_type, MessageType::Json);
    assert_eq!(dec_payload, inner_payload);
}

// ── HTTP request tests ──────────────────────────────────────────────────────

#[test]
fn test_http_request_roundtrip() {
    let method = HttpMethod::Post;
    let path = "/api/v1/data";
    let query = "key=value&foo=bar";
    let headers = vec![("Content-Type", "application/json"), ("X-Custom", "test")];
    let body = b"{\"data\":true}";
    let encoded = encode_http_request(method, path, query, &headers, body);
    let (dec_method, dec_path, dec_query, dec_headers, dec_body) =
        decode_http_request(&encoded).unwrap();
    assert_eq!(dec_method, HttpMethod::Post);
    assert_eq!(dec_path, path);
    assert_eq!(dec_query, query);
    assert_eq!(dec_headers.len(), 2);
    assert_eq!(dec_headers[0].0, "Content-Type");
    assert_eq!(dec_headers[0].1, "application/json");
    assert_eq!(dec_headers[1].0, "X-Custom");
    assert_eq!(dec_headers[1].1, "test");
    assert_eq!(dec_body, body);
}

#[test]
fn test_http_request_empty_body() {
    let encoded = encode_http_request(HttpMethod::Get, "", "", &[], b"");
    let (dec_method, dec_path, dec_query, dec_headers, dec_body) =
        decode_http_request(&encoded).unwrap();
    assert_eq!(dec_method, HttpMethod::Get);
    assert_eq!(dec_path, "");
    assert_eq!(dec_query, "");
    assert!(dec_headers.is_empty());
    assert!(dec_body.is_empty());
}

#[test]
fn test_http_request_all_methods() {
    let expected_methods = [
        HttpMethod::Get,
        HttpMethod::Post,
        HttpMethod::Put,
        HttpMethod::Delete,
        HttpMethod::Patch,
        HttpMethod::Head,
        HttpMethod::Options,
    ];
    for i in 0u8..=6 {
        let method = HttpMethod::try_from(i).unwrap();
        let encoded = encode_http_request(method, "/test", "", &[], b"");
        let (dec_method, _, _, _, _) = decode_http_request(&encoded).unwrap();
        assert_eq!(dec_method, expected_methods[i as usize]);
    }
}

// ── HTTP response tests ─────────────────────────────────────────────────────

#[test]
fn test_http_response_roundtrip() {
    let headers = vec![("Content-Type", "text/html"), ("X-Req-Id", "abc123")];
    let body = b"<html>OK</html>";
    let encoded = encode_http_response(200, &headers, body);
    let (dec_status, dec_headers, dec_body) = decode_http_response(&encoded).unwrap();
    assert_eq!(dec_status, 200);
    assert_eq!(dec_headers.len(), 2);
    assert_eq!(dec_headers[0].0, "Content-Type");
    assert_eq!(dec_headers[0].1, "text/html");
    assert_eq!(dec_headers[1].0, "X-Req-Id");
    assert_eq!(dec_headers[1].1, "abc123");
    assert_eq!(dec_body, body);
}

#[test]
fn test_http_response_empty() {
    let encoded = encode_http_response(204, &[], b"");
    let (dec_status, dec_headers, dec_body) = decode_http_response(&encoded).unwrap();
    assert_eq!(dec_status, 204);
    assert!(dec_headers.is_empty());
    assert!(dec_body.is_empty());
}

// ── HttpMethod tests ────────────────────────────────────────────────────────

#[test]
fn test_method_from_str() {
    assert_eq!(HttpMethod::from_str("GET").unwrap(), HttpMethod::Get);
    assert_eq!(HttpMethod::from_str("post").unwrap(), HttpMethod::Post);
    assert_eq!(HttpMethod::from_str("Delete").unwrap(), HttpMethod::Delete);
}

#[test]
fn test_method_to_str() {
    assert_eq!(HttpMethod::Get.as_str(), "GET");
    assert_eq!(HttpMethod::Post.as_str(), "POST");
    assert_eq!(HttpMethod::Put.as_str(), "PUT");
    assert_eq!(HttpMethod::Delete.as_str(), "DELETE");
    assert_eq!(HttpMethod::Patch.as_str(), "PATCH");
    assert_eq!(HttpMethod::Head.as_str(), "HEAD");
    assert_eq!(HttpMethod::Options.as_str(), "OPTIONS");
}
