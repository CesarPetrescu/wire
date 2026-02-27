//! Integration tests — spin up Controller + SubController and verify
//! all message types flow bidirectionally over a single WebSocket.

use serde_json::json;
use std::io::{Read, Write};
use tokio::time::{timeout, Duration};
use wire_rs::controller::{Controller, WireMessage as CtrlMsg};
use wire_rs::subcontroller::{SubController, WireMessage as SubMsg};

const SECRET: &str = "test-secret-42";

async fn make_pair(port: u16) -> (Controller, SubController) {
    let mut ctrl = Controller::new("127.0.0.1", port, SECRET);
    ctrl.start().await.expect("controller start");

    // Small delay for the listener to be ready
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut sub = SubController::new("127.0.0.1", port, SECRET);
    sub.connect().await.expect("subcontroller connect");

    tokio::time::sleep(Duration::from_millis(50)).await;
    (ctrl, sub)
}

#[tokio::test]
async fn test_auth_success() {
    let (ctrl, sub) = make_pair(29001).await;
    let fps = ctrl.peer_fingerprints().await;
    assert_eq!(fps.len(), 1);
    assert_eq!(fps[0], sub.fingerprint().unwrap());
    assert!(sub.controller_fingerprint().await.is_some());
}

#[tokio::test]
async fn test_auth_bad_secret() {
    let mut ctrl = Controller::new("127.0.0.1", 29002, "correct");
    ctrl.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut sub = SubController::new("127.0.0.1", 29002, "wrong");
    let result = sub.connect().await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("bad secret"), "Error was: {}", err);
}

#[tokio::test]
async fn test_json_sub_to_controller() {
    let (ctrl, sub) = make_pair(29003).await;
    let mut rx = ctrl.message_rx.unwrap();

    sub.send_json(&json!({"action": "ping", "value": 123}))
        .await
        .unwrap();

    let msg = timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();

    match msg {
        CtrlMsg::Json { data, .. } => {
            assert_eq!(data["action"], "ping");
            assert_eq!(data["value"], 123);
        }
        _ => panic!("Expected JSON message"),
    }
}

#[tokio::test]
async fn test_json_controller_to_sub() {
    let (ctrl, sub) = make_pair(29004).await;
    let mut rx = sub.message_rx.unwrap();
    let peer_fp = ctrl.peer_fingerprints().await[0].clone();

    ctrl.send_json(&peer_fp, &json!({"from": "controller"}))
        .await
        .unwrap();

    let msg = timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();

    match msg {
        SubMsg::Json { data } => {
            assert_eq!(data["from"], "controller");
        }
        _ => panic!("Expected JSON message"),
    }
}

#[tokio::test]
async fn test_binary_data() {
    let (ctrl, sub) = make_pair(29005).await;
    let mut rx = ctrl.message_rx.unwrap();

    let blob: Vec<u8> = (0..=255u8).collect::<Vec<u8>>().repeat(100);
    sub.send_binary(&blob).await.unwrap();

    let msg = timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();

    match msg {
        CtrlMsg::Binary { data, .. } => {
            assert_eq!(data, blob);
        }
        _ => panic!("Expected Binary message"),
    }
}

#[tokio::test]
async fn test_send_zip() {
    let (ctrl, sub) = make_pair(29006).await;
    let mut rx = ctrl.message_rx.unwrap();

    // Create a zip in memory
    let mut zip_buf = Vec::new();
    {
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut zip_buf));
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        zip.start_file("hello.txt", options).unwrap();
        zip.write_all(b"Hello from wire!").unwrap();
        zip.finish().unwrap();
    }

    sub.send_file("test.zip", &zip_buf, false).await.unwrap();

    let msg = timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();

    match msg {
        CtrlMsg::File { filename, data, .. } => {
            assert_eq!(filename, "test.zip");
            // Verify zip is valid
            let reader = std::io::Cursor::new(data);
            let mut archive = zip::ZipArchive::new(reader).unwrap();
            let mut file = archive.by_name("hello.txt").unwrap();
            let mut contents = String::new();
            file.read_to_string(&mut contents).unwrap();
            assert_eq!(contents, "Hello from wire!");
        }
        _ => panic!("Expected File message"),
    }
}

#[tokio::test]
async fn test_send_image() {
    let (ctrl, sub) = make_pair(29007).await;
    let mut rx = sub.message_rx.unwrap();
    let peer_fp = ctrl.peer_fingerprints().await[0].clone();

    // Minimal PNG (just the signature + some data for testing)
    let png_data = make_tiny_png();

    ctrl.send_file(&peer_fp, "photo.png", &png_data, true)
        .await
        .unwrap();

    let msg = timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();

    match msg {
        SubMsg::Image { filename, data } => {
            assert_eq!(filename, "photo.png");
            assert_eq!(data, png_data);
            assert_eq!(&data[..4], b"\x89PNG");
        }
        _ => panic!("Expected Image message"),
    }
}

#[tokio::test]
async fn test_large_binary_streaming() {
    let (ctrl, sub) = make_pair(29008).await;
    let mut rx = ctrl.message_rx.unwrap();

    // 10 MB blob — will be streamed
    let big: Vec<u8> = (0..10 * 1024 * 1024).map(|i| (i % 256) as u8).collect();
    sub.send_binary(&big).await.unwrap();

    let msg = timeout(Duration::from_secs(30), rx.recv())
        .await
        .unwrap()
        .unwrap();

    match msg {
        CtrlMsg::Binary { data, .. } => {
            assert_eq!(data.len(), big.len());
            assert_eq!(data, big);
        }
        _ => panic!("Expected Binary message"),
    }
}

#[tokio::test]
async fn test_rapid_fire_json() {
    let (ctrl, sub) = make_pair(29009).await;
    let mut rx = ctrl.message_rx.unwrap();
    let count = 100;

    for i in 0..count {
        sub.send_json(&json!({"seq": i})).await.unwrap();
    }

    let mut results = Vec::new();
    for _ in 0..count {
        let msg = timeout(Duration::from_secs(10), rx.recv())
            .await
            .unwrap()
            .unwrap();
        if let CtrlMsg::Json { data, .. } = msg {
            results.push(data["seq"].as_i64().unwrap());
        }
    }

    results.sort();
    let expected: Vec<i64> = (0..count).collect();
    assert_eq!(results, expected);
}

fn make_tiny_png() -> Vec<u8> {
    use flate2::write::ZlibEncoder;
    use flate2::Compression;
    use std::io::Write;

    let mut png = Vec::new();
    // PNG signature
    png.extend_from_slice(b"\x89PNG\r\n\x1a\n");

    // IHDR: 1x1 pixel, 8-bit RGB
    let mut ihdr_data = Vec::new();
    ihdr_data.extend_from_slice(&1u32.to_be_bytes()); // width
    ihdr_data.extend_from_slice(&1u32.to_be_bytes()); // height
    ihdr_data.push(8); // bit depth
    ihdr_data.push(2); // color type (RGB)
    ihdr_data.push(0); // compression
    ihdr_data.push(0); // filter
    ihdr_data.push(0); // interlace

    // IHDR chunk
    let ihdr_len = (ihdr_data.len() as u32).to_be_bytes();
    let mut ihdr_crc_input = Vec::new();
    ihdr_crc_input.extend_from_slice(b"IHDR");
    ihdr_crc_input.extend_from_slice(&ihdr_data);
    let ihdr_crc = simple_crc32(&ihdr_crc_input);
    png.extend_from_slice(&ihdr_len);
    png.extend_from_slice(b"IHDR");
    png.extend_from_slice(&ihdr_data);
    png.extend_from_slice(&ihdr_crc.to_be_bytes());

    // IDAT: compressed pixel data (filter=0, R=255, G=0, B=0)
    let raw_pixel = [0u8, 255, 0, 0]; // filter byte + RGB
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&raw_pixel).unwrap();
    let compressed = encoder.finish().unwrap();

    let mut idat_crc_input = Vec::new();
    idat_crc_input.extend_from_slice(b"IDAT");
    idat_crc_input.extend_from_slice(&compressed);
    let idat_crc = simple_crc32(&idat_crc_input);
    png.extend_from_slice(&(compressed.len() as u32).to_be_bytes());
    png.extend_from_slice(b"IDAT");
    png.extend_from_slice(&compressed);
    png.extend_from_slice(&idat_crc.to_be_bytes());

    // IEND
    let iend_crc = simple_crc32(b"IEND");
    png.extend_from_slice(&0u32.to_be_bytes());
    png.extend_from_slice(b"IEND");
    png.extend_from_slice(&iend_crc.to_be_bytes());

    png
}

/// Simple CRC32 (IEEE) implementation for PNG chunks.
fn simple_crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFFFFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB88320;
            } else {
                crc >>= 1;
            }
        }
    }
    crc ^ 0xFFFFFFFF
}
