"""
Certificate generation and management.

On startup each node generates a self-signed cert + private key.
After the initial handshake, both sides pin the peer's cert fingerprint
so future reconnections are verified against the same identity.
"""

import datetime
import hashlib
import os
import ssl
import tempfile
from dataclasses import dataclass
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID


@dataclass
class CertBundle:
    """Holds paths and data for a node's TLS identity."""

    cert_path: str
    key_path: str
    cert_pem: bytes
    key_pem: bytes
    fingerprint: str  # SHA-256 hex digest of DER cert


def generate_self_signed_cert(
    common_name: str = "wire-node",
    cert_dir: str | None = None,
) -> CertBundle:
    """Generate an ECDSA P-256 self-signed certificate.

    Returns a CertBundle with paths to the PEM files and the cert fingerprint.
    """
    if cert_dir is None:
        cert_dir = tempfile.mkdtemp(prefix="wire_certs_")
    else:
        os.makedirs(cert_dir, exist_ok=True)

    # Generate ECDSA private key
    private_key = ec.generate_private_key(ec.SECP256R1())

    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, common_name),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Wire"),
    ])

    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.utcnow())
        .not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(days=365))
        .add_extension(
            x509.SubjectAlternativeName([
                x509.DNSName("localhost"),
                x509.IPAddress(
                    __import__("ipaddress").IPv4Address("127.0.0.1")
                ),
            ]),
            critical=False,
        )
        .sign(private_key, hashes.SHA256())
    )

    cert_pem = cert.public_bytes(serialization.Encoding.PEM)
    key_pem = private_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )

    cert_path = os.path.join(cert_dir, f"{common_name}.crt")
    key_path = os.path.join(cert_dir, f"{common_name}.key")

    with open(cert_path, "wb") as f:
        f.write(cert_pem)
    with open(key_path, "wb") as f:
        f.write(key_pem)

    fingerprint = get_cert_fingerprint(cert_pem)

    return CertBundle(
        cert_path=cert_path,
        key_path=key_path,
        cert_pem=cert_pem,
        key_pem=key_pem,
        fingerprint=fingerprint,
    )


def get_cert_fingerprint(cert_pem: bytes) -> str:
    """Get SHA-256 fingerprint of a PEM certificate."""
    from cryptography.x509 import load_pem_x509_certificate

    cert = load_pem_x509_certificate(cert_pem)
    der = cert.public_bytes(serialization.Encoding.DER)
    return hashlib.sha256(der).hexdigest()


def create_ssl_context_server(bundle: CertBundle) -> ssl.SSLContext:
    """Create an SSL context for the server (controller) side."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(bundle.cert_path, bundle.key_path)
    # Don't require client cert at TLS level — we do mutual auth at
    # the application layer via the AUTH handshake so we can pin certs
    # without needing a shared CA.
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def create_ssl_context_client(bundle: CertBundle) -> ssl.SSLContext:
    """Create an SSL context for the client (subcontroller) side."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_cert_chain(bundle.cert_path, bundle.key_path)
    # We verify the server's cert at the application layer via fingerprint
    # pinning, not via CA trust.
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx
