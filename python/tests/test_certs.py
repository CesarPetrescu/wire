"""Unit tests for certificate generation and management."""

import os
import tempfile

import pytest

from wire.certs import (
    CertBundle,
    create_ssl_context_client,
    create_ssl_context_server,
    generate_self_signed_cert,
    get_cert_fingerprint,
)


class TestCertGeneration:
    def test_generates_files(self):
        bundle = generate_self_signed_cert(common_name="test-node")
        assert os.path.isfile(bundle.cert_path)
        assert os.path.isfile(bundle.key_path)
        assert bundle.cert_pem.startswith(b"-----BEGIN CERTIFICATE-----")
        assert bundle.key_pem.startswith(b"-----BEGIN PRIVATE KEY-----")

    def test_fingerprint_stable(self):
        bundle = generate_self_signed_cert(common_name="stable-test")
        fp1 = get_cert_fingerprint(bundle.cert_pem)
        fp2 = get_cert_fingerprint(bundle.cert_pem)
        assert fp1 == fp2
        assert len(fp1) == 64  # SHA-256 hex

    def test_different_certs_different_fingerprints(self):
        b1 = generate_self_signed_cert(common_name="node-a")
        b2 = generate_self_signed_cert(common_name="node-b")
        assert b1.fingerprint != b2.fingerprint

    def test_custom_cert_dir(self):
        with tempfile.TemporaryDirectory() as d:
            bundle = generate_self_signed_cert(common_name="custom", cert_dir=d)
            assert bundle.cert_path.startswith(d)
            assert bundle.key_path.startswith(d)

    def test_ssl_context_server(self):
        bundle = generate_self_signed_cert(common_name="srv")
        ctx = create_ssl_context_server(bundle)
        assert ctx is not None

    def test_ssl_context_client(self):
        bundle = generate_self_signed_cert(common_name="cli")
        ctx = create_ssl_context_client(bundle)
        assert ctx is not None
