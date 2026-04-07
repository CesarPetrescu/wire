use wire_rs::certs::*;

#[test]
fn test_generates_valid_cert() {
    let bundle = generate_self_signed_cert("test-node").unwrap();
    assert!(bundle.cert_pem.contains("BEGIN CERTIFICATE"));
    assert!(bundle.cert_pem.contains("END CERTIFICATE"));
    assert!(bundle.key_pem.contains("BEGIN PRIVATE KEY"));
    // Fingerprint is 64 hex chars (SHA-256)
    assert_eq!(bundle.fingerprint.len(), 64);
    assert!(bundle.fingerprint.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn test_fingerprint_stable() {
    let bundle = generate_self_signed_cert("stable").unwrap();
    let fp1 = get_cert_fingerprint_from_pem(&bundle.cert_pem).unwrap();
    let fp2 = get_cert_fingerprint_from_pem(&bundle.cert_pem).unwrap();
    assert_eq!(fp1, fp2);
    assert_eq!(fp1, bundle.fingerprint);
}

#[test]
fn test_different_certs_different_fingerprints() {
    let a = generate_self_signed_cert("node-a").unwrap();
    let b = generate_self_signed_cert("node-b").unwrap();
    assert_ne!(a.fingerprint, b.fingerprint);
}

#[test]
fn test_server_config() {
    let bundle = generate_self_signed_cert("srv").unwrap();
    assert!(create_server_config(&bundle).is_ok());
}

#[test]
fn test_client_config() {
    let bundle = generate_self_signed_cert("cli").unwrap();
    assert!(create_client_config(&bundle).is_ok());
}
