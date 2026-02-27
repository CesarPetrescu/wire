//! Comprehensive star topology integration tests — 1 Controller + 5 SubControllers.
//!
//! Test matrix:
//!   - All 20 directional pairs (every sub → every other sub) for each data type
//!   - 4 data types: JSON, binary, file (with checksum), image (with checksum)
//!   - 3 size tiers: 256B (small), 5MB (streamed), 16MB (large streamed)
//!   - Direct controller ↔ sub communication
//!   - Peer discovery and leave notifications

#![allow(unused_variables, unused_mut)]

use serde_json::{json, Value};
use std::collections::HashSet;
use std::io::Write;
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};
use wire_rs::controller::{Controller, WireMessage as CtrlMsg};
use wire_rs::subcontroller::{SubController, WireMessage as SubMsg};

const SECRET: &str = "star-test-secret-42";
const NUM_SUBS: usize = 5;

/// Create 1 Controller + N SubControllers.  Returns (ctrl, subs, sub_rxs).
async fn make_star(
    base_port: u16,
) -> (
    Controller,
    Vec<SubController>,
    Vec<mpsc::UnboundedReceiver<SubMsg>>,
) {
    let mut ctrl = Controller::new("127.0.0.1", base_port, SECRET);
    ctrl.start().await.expect("controller start");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut subs = Vec::new();
    let mut rxs = Vec::new();

    for _ in 0..NUM_SUBS {
        let mut sub = SubController::new("127.0.0.1", base_port, SECRET);
        let rx = sub.message_rx.take().unwrap();
        sub.connect().await.expect("sub connect");
        tokio::time::sleep(Duration::from_millis(100)).await;
        subs.push(sub);
        rxs.push(rx);
    }

    // Let peer notifications propagate
    tokio::time::sleep(Duration::from_millis(500)).await;

    (ctrl, subs, rxs)
}

fn make_binary_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
}

fn make_json_data(size_hint: usize) -> Value {
    let count = std::cmp::max(1, size_hint / 20);
    let mut map = serde_json::Map::new();
    for i in 0..count {
        map.insert(format!("k_{:06}", i), Value::String(format!("v_{:06}", i)));
    }
    Value::Object(map)
}

fn make_file_data(size: usize) -> (String, Vec<u8>) {
    let mut zip_buf = Vec::new();
    {
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut zip_buf));
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        zip.start_file("payload.bin", options).unwrap();
        zip.write_all(&make_binary_data(size)).unwrap();
        zip.finish().unwrap();
    }
    ("test_transfer.zip".to_string(), zip_buf)
}

fn make_tiny_png() -> Vec<u8> {
    use flate2::write::ZlibEncoder;
    use flate2::Compression;

    let mut png = Vec::new();
    png.extend_from_slice(b"\x89PNG\r\n\x1a\n");

    let mut ihdr_data = Vec::new();
    ihdr_data.extend_from_slice(&1u32.to_be_bytes());
    ihdr_data.extend_from_slice(&1u32.to_be_bytes());
    ihdr_data.push(8);
    ihdr_data.push(2);
    ihdr_data.push(0);
    ihdr_data.push(0);
    ihdr_data.push(0);

    let ihdr_len = (ihdr_data.len() as u32).to_be_bytes();
    let mut ihdr_crc_input = Vec::new();
    ihdr_crc_input.extend_from_slice(b"IHDR");
    ihdr_crc_input.extend_from_slice(&ihdr_data);
    let ihdr_crc = simple_crc32(&ihdr_crc_input);
    png.extend_from_slice(&ihdr_len);
    png.extend_from_slice(b"IHDR");
    png.extend_from_slice(&ihdr_data);
    png.extend_from_slice(&ihdr_crc.to_be_bytes());

    let raw_pixel = [0u8, 255, 0, 0];
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

    let iend_crc = simple_crc32(b"IEND");
    png.extend_from_slice(&0u32.to_be_bytes());
    png.extend_from_slice(b"IEND");
    png.extend_from_slice(&iend_crc.to_be_bytes());

    png
}

fn make_image_data(size: usize) -> (String, Vec<u8>) {
    let mut data = make_tiny_png();
    if size > data.len() {
        data.extend(make_binary_data(size - data.len()));
    }
    ("test_image.png".to_string(), data)
}

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

// ===========================================================================
// Connection & peer discovery tests
// ===========================================================================

#[tokio::test]
async fn test_star_all_subs_connected() {
    let (ctrl, subs, _rxs) = make_star(30000).await;
    let fps = ctrl.peer_fingerprints().await;
    assert_eq!(fps.len(), NUM_SUBS);
    for sub in &subs {
        assert!(sub.controller_fingerprint().await.is_some());
    }
}

#[tokio::test]
async fn test_star_peer_discovery() {
    let (ctrl, subs, _rxs) = make_star(30001).await;
    for (i, sub) in subs.iter().enumerate() {
        let peers = sub.known_peers().await;
        assert_eq!(
            peers.len(),
            NUM_SUBS - 1,
            "Sub {} has {} peers, expected {}",
            i,
            peers.len(),
            NUM_SUBS - 1
        );
        let own_fp = sub.fingerprint().unwrap();
        assert!(!peers.contains(&own_fp));
        for (j, other) in subs.iter().enumerate() {
            if i != j {
                let other_fp = other.fingerprint().unwrap();
                assert!(
                    peers.contains(&other_fp),
                    "Sub {} doesn't know about sub {}",
                    i,
                    j
                );
            }
        }
    }
}

#[tokio::test]
async fn test_star_peer_leave() {
    let (ctrl, mut subs, mut rxs) = make_star(30002).await;
    let leaving_fp = subs[4].fingerprint().unwrap();

    // Verify all subs know about sub 4
    for i in 0..4 {
        let peers = subs[i].known_peers().await;
        assert!(peers.contains(&leaving_fp));
    }

    subs[4].disconnect().await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    for i in 0..4 {
        let peers = subs[i].known_peers().await;
        assert!(!peers.contains(&leaving_fp));
    }

    assert_eq!(ctrl.peer_fingerprints().await.len(), NUM_SUBS - 1);
}

// ===========================================================================
// 256B small tests — all 20 pairs × all 4 data types
// ===========================================================================

#[tokio::test]
async fn test_all_pairs_json_256b() {
    let (_ctrl, subs, mut rxs) = make_star(30010).await;
    let expected = NUM_SUBS * (NUM_SUBS - 1);
    let test_data = make_json_data(256);

    // Send from every sub to every other sub
    for i in 0..NUM_SUBS {
        for j in 0..NUM_SUBS {
            if i != j {
                let dest_fp = subs[j].fingerprint().unwrap();
                subs[i]
                    .send_json_to_peer(&dest_fp, &json!({"from": i, "to": j, "data": test_data}))
                    .await
                    .unwrap();
            }
        }
    }

    // Collect all messages across all receivers
    let mut total_received = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    while total_received < expected && tokio::time::Instant::now() < deadline {
        for rx in rxs.iter_mut() {
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(SubMsg::RelayJson { source_fp, data })) => {
                    assert!(data.get("from").is_some());
                    assert!(data.get("to").is_some());
                    total_received += 1;
                }
                Ok(Some(SubMsg::PeerJoined { .. })) | Ok(Some(SubMsg::PeerLeft { .. })) => {}
                _ => {}
            }
        }
    }
    assert_eq!(total_received, expected);
}

#[tokio::test]
async fn test_all_pairs_binary_256b() {
    let (_ctrl, subs, mut rxs) = make_star(30011).await;
    let expected = NUM_SUBS * (NUM_SUBS - 1);
    let blob = make_binary_data(256);

    for i in 0..NUM_SUBS {
        for j in 0..NUM_SUBS {
            if i != j {
                let dest_fp = subs[j].fingerprint().unwrap();
                subs[i].send_binary_to_peer(&dest_fp, &blob).await.unwrap();
            }
        }
    }

    let mut total_received = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    while total_received < expected && tokio::time::Instant::now() < deadline {
        for rx in rxs.iter_mut() {
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(SubMsg::RelayBinary { data, .. })) => {
                    assert_eq!(data, blob);
                    total_received += 1;
                }
                Ok(Some(SubMsg::PeerJoined { .. })) | Ok(Some(SubMsg::PeerLeft { .. })) => {}
                _ => {}
            }
        }
    }
    assert_eq!(total_received, expected);
}

#[tokio::test]
async fn test_all_pairs_file_256b() {
    let (_ctrl, subs, mut rxs) = make_star(30012).await;
    let expected = NUM_SUBS * (NUM_SUBS - 1);
    let (fname, fdata) = make_file_data(256);

    for i in 0..NUM_SUBS {
        for j in 0..NUM_SUBS {
            if i != j {
                let dest_fp = subs[j].fingerprint().unwrap();
                subs[i]
                    .send_file_to_peer(&dest_fp, &fname, &fdata, false)
                    .await
                    .unwrap();
            }
        }
    }

    let mut total_received = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    while total_received < expected && tokio::time::Instant::now() < deadline {
        for rx in rxs.iter_mut() {
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(SubMsg::RelayFile {
                    filename, data, ..
                })) => {
                    assert_eq!(filename, fname);
                    assert_eq!(data, fdata);
                    total_received += 1;
                }
                Ok(Some(SubMsg::PeerJoined { .. })) | Ok(Some(SubMsg::PeerLeft { .. })) => {}
                _ => {}
            }
        }
    }
    assert_eq!(total_received, expected);
}

#[tokio::test]
async fn test_all_pairs_image_256b() {
    let (_ctrl, subs, mut rxs) = make_star(30013).await;
    let expected = NUM_SUBS * (NUM_SUBS - 1);
    let (fname, fdata) = make_image_data(256);

    for i in 0..NUM_SUBS {
        for j in 0..NUM_SUBS {
            if i != j {
                let dest_fp = subs[j].fingerprint().unwrap();
                subs[i]
                    .send_file_to_peer(&dest_fp, &fname, &fdata, true)
                    .await
                    .unwrap();
            }
        }
    }

    let mut total_received = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    while total_received < expected && tokio::time::Instant::now() < deadline {
        for rx in rxs.iter_mut() {
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(SubMsg::RelayImage {
                    filename, data, ..
                })) => {
                    assert_eq!(filename, fname);
                    assert_eq!(data, fdata);
                    assert_eq!(&data[..4], b"\x89PNG");
                    total_received += 1;
                }
                Ok(Some(SubMsg::PeerJoined { .. })) | Ok(Some(SubMsg::PeerLeft { .. })) => {}
                _ => {}
            }
        }
    }
    assert_eq!(total_received, expected);
}

// ===========================================================================
// 5MB streamed tests — all 20 pairs × all 4 data types
// ===========================================================================

#[tokio::test]
async fn test_all_pairs_json_5mb() {
    let (_ctrl, subs, mut rxs) = make_star(30020).await;
    let expected = NUM_SUBS * (NUM_SUBS - 1);
    let test_data = make_json_data(5 * 1024 * 1024);
    let encoded_size = serde_json::to_vec(&test_data).unwrap().len();

    for i in 0..NUM_SUBS {
        for j in 0..NUM_SUBS {
            if i != j {
                let dest_fp = subs[j].fingerprint().unwrap();
                subs[i]
                    .send_json_to_peer(&dest_fp, &test_data)
                    .await
                    .unwrap();
            }
        }
    }

    let mut total_received = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);

    while total_received < expected && tokio::time::Instant::now() < deadline {
        for rx in rxs.iter_mut() {
            match tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
                Ok(Some(SubMsg::RelayJson { data, .. })) => {
                    let sz = serde_json::to_vec(&data).unwrap().len();
                    assert_eq!(sz, encoded_size);
                    total_received += 1;
                }
                Ok(Some(SubMsg::PeerJoined { .. })) | Ok(Some(SubMsg::PeerLeft { .. })) => {}
                _ => {}
            }
        }
    }
    assert_eq!(total_received, expected);
}

#[tokio::test]
async fn test_all_pairs_binary_5mb() {
    let (_ctrl, subs, mut rxs) = make_star(30021).await;
    let expected = NUM_SUBS * (NUM_SUBS - 1);
    let size = 5 * 1024 * 1024;
    let blob = make_binary_data(size);

    for i in 0..NUM_SUBS {
        for j in 0..NUM_SUBS {
            if i != j {
                let dest_fp = subs[j].fingerprint().unwrap();
                subs[i].send_binary_to_peer(&dest_fp, &blob).await.unwrap();
            }
        }
    }

    let mut total_received = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);

    while total_received < expected && tokio::time::Instant::now() < deadline {
        for rx in rxs.iter_mut() {
            match tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
                Ok(Some(SubMsg::RelayBinary { data, .. })) => {
                    assert_eq!(data.len(), size);
                    total_received += 1;
                }
                Ok(Some(SubMsg::PeerJoined { .. })) | Ok(Some(SubMsg::PeerLeft { .. })) => {}
                _ => {}
            }
        }
    }
    assert_eq!(total_received, expected);
}

#[tokio::test]
async fn test_all_pairs_file_5mb() {
    let (_ctrl, subs, mut rxs) = make_star(30022).await;
    let expected = NUM_SUBS * (NUM_SUBS - 1);
    let (fname, fdata) = make_file_data(5 * 1024 * 1024);
    let fdata_len = fdata.len();

    for i in 0..NUM_SUBS {
        for j in 0..NUM_SUBS {
            if i != j {
                let dest_fp = subs[j].fingerprint().unwrap();
                subs[i]
                    .send_file_to_peer(&dest_fp, &fname, &fdata, false)
                    .await
                    .unwrap();
            }
        }
    }

    let mut total_received = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);

    while total_received < expected && tokio::time::Instant::now() < deadline {
        for rx in rxs.iter_mut() {
            match tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
                Ok(Some(SubMsg::RelayFile {
                    filename, data, ..
                })) => {
                    assert_eq!(filename, fname);
                    assert_eq!(data.len(), fdata_len);
                    total_received += 1;
                }
                Ok(Some(SubMsg::PeerJoined { .. })) | Ok(Some(SubMsg::PeerLeft { .. })) => {}
                _ => {}
            }
        }
    }
    assert_eq!(total_received, expected);
}

#[tokio::test]
async fn test_all_pairs_image_5mb() {
    let (_ctrl, subs, mut rxs) = make_star(30023).await;
    let expected = NUM_SUBS * (NUM_SUBS - 1);
    let (fname, fdata) = make_image_data(5 * 1024 * 1024);
    let fdata_len = fdata.len();

    for i in 0..NUM_SUBS {
        for j in 0..NUM_SUBS {
            if i != j {
                let dest_fp = subs[j].fingerprint().unwrap();
                subs[i]
                    .send_file_to_peer(&dest_fp, &fname, &fdata, true)
                    .await
                    .unwrap();
            }
        }
    }

    let mut total_received = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);

    while total_received < expected && tokio::time::Instant::now() < deadline {
        for rx in rxs.iter_mut() {
            match tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
                Ok(Some(SubMsg::RelayImage {
                    filename, data, ..
                })) => {
                    assert_eq!(filename, fname);
                    assert_eq!(data.len(), fdata_len);
                    assert_eq!(&data[..4], b"\x89PNG");
                    total_received += 1;
                }
                Ok(Some(SubMsg::PeerJoined { .. })) | Ok(Some(SubMsg::PeerLeft { .. })) => {}
                _ => {}
            }
        }
    }
    assert_eq!(total_received, expected);
}

// ===========================================================================
// 16MB tests — all 20 pairs × all 4 data types
// ===========================================================================

#[tokio::test]
async fn test_all_pairs_json_16mb() {
    let (_ctrl, subs, mut rxs) = make_star(30030).await;
    let expected = NUM_SUBS * (NUM_SUBS - 1);
    let test_data = make_json_data(16 * 1024 * 1024);
    let encoded_size = serde_json::to_vec(&test_data).unwrap().len();

    for i in 0..NUM_SUBS {
        for j in 0..NUM_SUBS {
            if i != j {
                let dest_fp = subs[j].fingerprint().unwrap();
                subs[i]
                    .send_json_to_peer(&dest_fp, &test_data)
                    .await
                    .unwrap();
            }
        }
    }

    let mut total_received = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);

    while total_received < expected && tokio::time::Instant::now() < deadline {
        for rx in rxs.iter_mut() {
            match tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
                Ok(Some(SubMsg::RelayJson { data, .. })) => {
                    let sz = serde_json::to_vec(&data).unwrap().len();
                    assert_eq!(sz, encoded_size);
                    total_received += 1;
                }
                Ok(Some(SubMsg::PeerJoined { .. })) | Ok(Some(SubMsg::PeerLeft { .. })) => {}
                _ => {}
            }
        }
    }
    assert_eq!(total_received, expected);
}

#[tokio::test]
async fn test_all_pairs_binary_16mb() {
    let (_ctrl, subs, mut rxs) = make_star(30031).await;
    let expected = NUM_SUBS * (NUM_SUBS - 1);
    let size: usize = 16 * 1024 * 1024;
    let blob = make_binary_data(size);

    for i in 0..NUM_SUBS {
        for j in 0..NUM_SUBS {
            if i != j {
                let dest_fp = subs[j].fingerprint().unwrap();
                subs[i].send_binary_to_peer(&dest_fp, &blob).await.unwrap();
            }
        }
    }

    let mut total_received = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);

    while total_received < expected && tokio::time::Instant::now() < deadline {
        for rx in rxs.iter_mut() {
            match tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
                Ok(Some(SubMsg::RelayBinary { data, .. })) => {
                    assert_eq!(data.len(), size);
                    total_received += 1;
                }
                Ok(Some(SubMsg::PeerJoined { .. })) | Ok(Some(SubMsg::PeerLeft { .. })) => {}
                _ => {}
            }
        }
    }
    assert_eq!(total_received, expected);
}

#[tokio::test]
async fn test_all_pairs_file_16mb() {
    let (_ctrl, subs, mut rxs) = make_star(30032).await;
    let expected = NUM_SUBS * (NUM_SUBS - 1);
    let (fname, fdata) = make_file_data(16 * 1024 * 1024);
    let fdata_len = fdata.len();

    for i in 0..NUM_SUBS {
        for j in 0..NUM_SUBS {
            if i != j {
                let dest_fp = subs[j].fingerprint().unwrap();
                subs[i]
                    .send_file_to_peer(&dest_fp, &fname, &fdata, false)
                    .await
                    .unwrap();
            }
        }
    }

    let mut total_received = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);

    while total_received < expected && tokio::time::Instant::now() < deadline {
        for rx in rxs.iter_mut() {
            match tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
                Ok(Some(SubMsg::RelayFile {
                    filename, data, ..
                })) => {
                    assert_eq!(filename, fname);
                    assert_eq!(data.len(), fdata_len);
                    total_received += 1;
                }
                Ok(Some(SubMsg::PeerJoined { .. })) | Ok(Some(SubMsg::PeerLeft { .. })) => {}
                _ => {}
            }
        }
    }
    assert_eq!(total_received, expected);
}

#[tokio::test]
async fn test_all_pairs_image_16mb() {
    let (_ctrl, subs, mut rxs) = make_star(30033).await;
    let expected = NUM_SUBS * (NUM_SUBS - 1);
    let (fname, fdata) = make_image_data(16 * 1024 * 1024);
    let fdata_len = fdata.len();

    for i in 0..NUM_SUBS {
        for j in 0..NUM_SUBS {
            if i != j {
                let dest_fp = subs[j].fingerprint().unwrap();
                subs[i]
                    .send_file_to_peer(&dest_fp, &fname, &fdata, true)
                    .await
                    .unwrap();
            }
        }
    }

    let mut total_received = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);

    while total_received < expected && tokio::time::Instant::now() < deadline {
        for rx in rxs.iter_mut() {
            match tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
                Ok(Some(SubMsg::RelayImage {
                    filename, data, ..
                })) => {
                    assert_eq!(filename, fname);
                    assert_eq!(data.len(), fdata_len);
                    assert_eq!(&data[..4], b"\x89PNG");
                    total_received += 1;
                }
                Ok(Some(SubMsg::PeerJoined { .. })) | Ok(Some(SubMsg::PeerLeft { .. })) => {}
                _ => {}
            }
        }
    }
    assert_eq!(total_received, expected);
}

// ===========================================================================
// Direct controller ↔ sub tests (all 5 subs, all types, both directions)
// ===========================================================================

#[tokio::test]
async fn test_direct_controller_json_to_each_sub() {
    let (ctrl, subs, mut rxs) = make_star(30040).await;

    for i in 0..NUM_SUBS {
        let fp = subs[i].fingerprint().unwrap();
        ctrl.send_json(&fp, &json!({"target": i, "msg": "hello"}))
            .await
            .unwrap();
    }

    for i in 0..NUM_SUBS {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let msg = timeout(Duration::from_secs(5), rxs[i].recv())
                .await
                .unwrap()
                .unwrap();
            match msg {
                SubMsg::Json { data } => {
                    assert_eq!(data["target"], i as i64);
                    assert_eq!(data["msg"], "hello");
                    break;
                }
                SubMsg::PeerJoined { .. } | SubMsg::PeerLeft { .. } => continue,
                _ => panic!("Unexpected message type for sub {}", i),
            }
        }
    }
}

#[tokio::test]
async fn test_direct_each_sub_json_to_controller() {
    let (ctrl, subs, _rxs) = make_star(30041).await;
    let mut ctrl_rx = ctrl.message_rx.unwrap();

    for i in 0..NUM_SUBS {
        subs[i]
            .send_json(&json!({"from_sub": i}))
            .await
            .unwrap();
    }

    let mut received = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);

    while received.len() < NUM_SUBS && tokio::time::Instant::now() < deadline {
        match timeout(Duration::from_secs(5), ctrl_rx.recv()).await {
            Ok(Some(CtrlMsg::Json { peer_fp, data })) => {
                received.push((peer_fp, data));
            }
            _ => {}
        }
    }

    assert_eq!(received.len(), NUM_SUBS);
    let fps: HashSet<String> = received.iter().map(|(fp, _)| fp.clone()).collect();
    for sub in &subs {
        assert!(fps.contains(&sub.fingerprint().unwrap()));
    }
}

#[tokio::test]
async fn test_direct_controller_binary_to_each_sub() {
    let (ctrl, subs, mut rxs) = make_star(30042).await;
    let blob = make_binary_data(256);

    for i in 0..NUM_SUBS {
        let fp = subs[i].fingerprint().unwrap();
        ctrl.send_binary(&fp, &blob).await.unwrap();
    }

    for i in 0..NUM_SUBS {
        loop {
            let msg = timeout(Duration::from_secs(5), rxs[i].recv())
                .await
                .unwrap()
                .unwrap();
            match msg {
                SubMsg::Binary { data } => {
                    assert_eq!(data, blob);
                    break;
                }
                SubMsg::PeerJoined { .. } | SubMsg::PeerLeft { .. } => continue,
                _ => panic!("Unexpected message type"),
            }
        }
    }
}

#[tokio::test]
async fn test_direct_each_sub_binary_to_controller() {
    let (ctrl, subs, _rxs) = make_star(30043).await;
    let mut ctrl_rx = ctrl.message_rx.unwrap();
    let blob = make_binary_data(256);

    for sub in &subs {
        sub.send_binary(&blob).await.unwrap();
    }

    let mut count = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);

    while count < NUM_SUBS && tokio::time::Instant::now() < deadline {
        match timeout(Duration::from_secs(5), ctrl_rx.recv()).await {
            Ok(Some(CtrlMsg::Binary { data, .. })) => {
                assert_eq!(data, blob);
                count += 1;
            }
            _ => {}
        }
    }
    assert_eq!(count, NUM_SUBS);
}

#[tokio::test]
async fn test_direct_controller_file_to_each_sub() {
    let (ctrl, subs, mut rxs) = make_star(30044).await;
    let (fname, fdata) = make_file_data(256);

    for i in 0..NUM_SUBS {
        let fp = subs[i].fingerprint().unwrap();
        ctrl.send_file(&fp, &fname, &fdata, false).await.unwrap();
    }

    for i in 0..NUM_SUBS {
        loop {
            let msg = timeout(Duration::from_secs(5), rxs[i].recv())
                .await
                .unwrap()
                .unwrap();
            match msg {
                SubMsg::File { filename, data } => {
                    assert_eq!(filename, fname);
                    assert_eq!(data, fdata);
                    break;
                }
                SubMsg::PeerJoined { .. } | SubMsg::PeerLeft { .. } => continue,
                _ => panic!("Unexpected message type"),
            }
        }
    }
}

#[tokio::test]
async fn test_direct_each_sub_file_to_controller() {
    let (ctrl, subs, _rxs) = make_star(30045).await;
    let mut ctrl_rx = ctrl.message_rx.unwrap();
    let (fname, fdata) = make_file_data(256);

    for sub in &subs {
        sub.send_file(&fname, &fdata, false).await.unwrap();
    }

    let mut count = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);

    while count < NUM_SUBS && tokio::time::Instant::now() < deadline {
        match timeout(Duration::from_secs(5), ctrl_rx.recv()).await {
            Ok(Some(CtrlMsg::File { filename, data, .. })) => {
                assert_eq!(filename, fname);
                assert_eq!(data, fdata);
                count += 1;
            }
            _ => {}
        }
    }
    assert_eq!(count, NUM_SUBS);
}

#[tokio::test]
async fn test_direct_controller_image_to_each_sub() {
    let (ctrl, subs, mut rxs) = make_star(30046).await;
    let (fname, fdata) = make_image_data(256);

    for i in 0..NUM_SUBS {
        let fp = subs[i].fingerprint().unwrap();
        ctrl.send_file(&fp, &fname, &fdata, true).await.unwrap();
    }

    for i in 0..NUM_SUBS {
        loop {
            let msg = timeout(Duration::from_secs(5), rxs[i].recv())
                .await
                .unwrap()
                .unwrap();
            match msg {
                SubMsg::Image { filename, data } => {
                    assert_eq!(filename, fname);
                    assert_eq!(&data[..4], b"\x89PNG");
                    break;
                }
                SubMsg::PeerJoined { .. } | SubMsg::PeerLeft { .. } => continue,
                _ => panic!("Unexpected message type"),
            }
        }
    }
}

#[tokio::test]
async fn test_direct_each_sub_image_to_controller() {
    let (ctrl, subs, _rxs) = make_star(30047).await;
    let mut ctrl_rx = ctrl.message_rx.unwrap();
    let (fname, fdata) = make_image_data(256);

    for sub in &subs {
        sub.send_file(&fname, &fdata, true).await.unwrap();
    }

    let mut count = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);

    while count < NUM_SUBS && tokio::time::Instant::now() < deadline {
        match timeout(Duration::from_secs(5), ctrl_rx.recv()).await {
            Ok(Some(CtrlMsg::Image { filename, data, .. })) => {
                assert_eq!(filename, fname);
                assert_eq!(&data[..4], b"\x89PNG");
                count += 1;
            }
            _ => {}
        }
    }
    assert_eq!(count, NUM_SUBS);
}
