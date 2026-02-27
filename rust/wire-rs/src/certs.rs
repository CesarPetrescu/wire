//! Certificate generation and management.
//!
//! Each node generates an ECDSA P-256 self-signed cert on startup.
//! After initial handshake, cert fingerprints are pinned.

use rcgen::{CertificateParams, KeyPair};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use sha2::{Digest, Sha256};
use std::io::BufReader;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CertError {
    #[error("Certificate generation error: {0}")]
    Generation(String),
    #[error("TLS config error: {0}")]
    TlsConfig(String),
}

#[derive(Clone)]
pub struct CertBundle {
    pub cert_pem: String,
    pub key_pem: String,
    pub cert_der: Vec<u8>,
    pub fingerprint: String,
}

/// Generate a self-signed ECDSA P-256 certificate.
pub fn generate_self_signed_cert(common_name: &str) -> Result<CertBundle, CertError> {
    let key_pair =
        KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).map_err(|e| CertError::Generation(e.to_string()))?;

    let mut params = CertificateParams::new(vec!["localhost".to_string()])
        .map_err(|e| CertError::Generation(e.to_string()))?;
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, common_name);
    params
        .distinguished_name
        .push(rcgen::DnType::OrganizationName, "Wire");

    let cert = params
        .self_signed(&key_pair)
        .map_err(|e| CertError::Generation(e.to_string()))?;

    let cert_pem = cert.pem();
    let key_pem = key_pair.serialize_pem();
    let cert_der = cert.der().to_vec();

    let fingerprint = hex::encode(Sha256::digest(&cert_der));

    Ok(CertBundle {
        cert_pem,
        key_pem,
        cert_der,
        fingerprint,
    })
}

/// Get SHA-256 fingerprint from PEM certificate bytes.
pub fn get_cert_fingerprint_from_pem(pem: &str) -> Result<String, CertError> {
    let mut reader = BufReader::new(pem.as_bytes());
    let certs: Vec<CertificateDer> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| CertError::TlsConfig(e.to_string()))?;

    if certs.is_empty() {
        return Err(CertError::TlsConfig("No certificate found in PEM".into()));
    }

    Ok(hex::encode(Sha256::digest(certs[0].as_ref())))
}

/// Create a rustls ServerConfig for the controller.
pub fn create_server_config(bundle: &CertBundle) -> Result<Arc<rustls::ServerConfig>, CertError> {
    let cert = parse_cert_pem(&bundle.cert_pem)?;
    let key = parse_key_pem(&bundle.key_pem)?;

    let config = rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|e| CertError::TlsConfig(e.to_string()))?
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
        .map_err(|e| CertError::TlsConfig(e.to_string()))?;

    Ok(Arc::new(config))
}

/// Create a rustls ClientConfig for the subcontroller.
pub fn create_client_config(bundle: &CertBundle) -> Result<Arc<rustls::ClientConfig>, CertError> {
    let cert = parse_cert_pem(&bundle.cert_pem)?;
    let key = parse_key_pem(&bundle.key_pem)?;

    let config = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|e| CertError::TlsConfig(e.to_string()))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerify))
        .with_client_auth_cert(vec![cert], key)
        .map_err(|e| CertError::TlsConfig(e.to_string()))?;

    Ok(Arc::new(config))
}

fn parse_cert_pem(pem: &str) -> Result<CertificateDer<'static>, CertError> {
    let mut reader = BufReader::new(pem.as_bytes());
    let certs: Vec<CertificateDer> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| CertError::TlsConfig(e.to_string()))?;
    certs
        .into_iter()
        .next()
        .ok_or_else(|| CertError::TlsConfig("No certificate in PEM".into()))
}

fn parse_key_pem(pem: &str) -> Result<PrivateKeyDer<'static>, CertError> {
    let mut reader = BufReader::new(pem.as_bytes());
    let keys: Vec<PrivatePkcs8KeyDer> = rustls_pemfile::pkcs8_private_keys(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| CertError::TlsConfig(e.to_string()))?;
    keys.into_iter()
        .next()
        .map(PrivateKeyDer::from)
        .ok_or_else(|| CertError::TlsConfig("No private key in PEM".into()))
}

/// A TLS certificate verifier that accepts any certificate.
/// We do our own pinning at the application layer.
#[derive(Debug)]
struct NoVerify;

impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ED25519,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_cert() {
        let bundle = generate_self_signed_cert("test-node").unwrap();
        assert!(bundle.cert_pem.contains("BEGIN CERTIFICATE"));
        assert!(bundle.key_pem.contains("BEGIN PRIVATE KEY"));
        assert_eq!(bundle.fingerprint.len(), 64);
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
    fn test_different_certs() {
        let a = generate_self_signed_cert("a").unwrap();
        let b = generate_self_signed_cert("b").unwrap();
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
}
