/**
 * Wire certificate management — ECDSA P-256 self-signed certs.
 */

import * as crypto from "crypto";
import * as tls from "tls";

export interface CertBundle {
  certPem: string;
  keyPem: string;
  fingerprint: string; // SHA-256 hex of DER-encoded cert
}

/**
 * Generate an ECDSA P-256 self-signed certificate.
 */
export function generateSelfSignedCert(commonName: string): CertBundle {
  // Generate ECDSA P-256 key pair
  const { publicKey, privateKey } = crypto.generateKeyPairSync("ec", {
    namedCurve: "prime256v1",
  });

  const keyPem = privateKey
    .export({ type: "pkcs8", format: "pem" })
    .toString();

  // Self-signed certificate using Node.js X509Certificate
  // Node 20+ has crypto.X509Certificate but not creation API.
  // We use the legacy openssl-style approach via tls.createSecureContext workaround.
  // Actually, the simplest cross-version approach: create cert with createCertificate-like logic.
  // Node doesn't have a built-in cert creation API, so we build a minimal ASN.1 DER cert.

  const certPem = createSelfSignedCert(commonName, publicKey, privateKey);
  const fingerprint = getCertFingerprint(certPem);

  return { certPem, keyPem, fingerprint };
}

/**
 * Get SHA-256 fingerprint of a PEM certificate.
 */
export function getCertFingerprint(certPem: string): string {
  const x509 = new crypto.X509Certificate(certPem);
  // x509.raw gives the DER bytes
  return crypto.createHash("sha256").update(x509.raw).digest("hex");
}

/**
 * Create a TLS secure context for client connections (no server cert verification).
 */
export function createClientTlsOptions(bundle: CertBundle): tls.ConnectionOptions {
  return {
    cert: bundle.certPem,
    key: bundle.keyPem,
    rejectUnauthorized: false,
    // Don't verify server cert at TLS level; we do fingerprint pinning in AUTH
  };
}

// ── Internal: ASN.1 DER certificate builder ─────────────────────────────────

function createSelfSignedCert(
  commonName: string,
  publicKey: crypto.KeyObject,
  privateKey: crypto.KeyObject,
): string {
  // Build a minimal self-signed X.509 v3 certificate in DER format

  const now = new Date();
  const notBefore = now;
  const notAfter = new Date(now.getTime() + 365 * 24 * 60 * 60 * 1000);

  // Subject/Issuer: CN=commonName, O=Wire
  const subject = buildName(commonName);

  // Public key info
  const pubKeyDer = publicKey.export({ type: "spki", format: "der" });

  // Build TBS (to-be-signed) certificate
  const serial = crypto.randomBytes(16);
  serial[0] &= 0x7f; // Ensure positive

  const tbs = buildTbsCertificate(
    serial,
    subject,
    subject, // self-signed: issuer = subject
    notBefore,
    notAfter,
    pubKeyDer,
  );

  // Sign with ECDSA SHA-256
  const signer = crypto.createSign("SHA256");
  signer.update(tbs);
  const signature = signer.sign(privateKey);

  // Wrap signature in BIT STRING
  const sigBitString = derBitString(signature);

  // Algorithm identifier: ecdsa-with-SHA256 (1.2.840.10045.4.3.2)
  const sigAlgId = derSequence(
    Buffer.from([0x06, 0x08, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x02]),
  );

  // Full certificate
  const cert = derSequence(Buffer.concat([tbs, sigAlgId, sigBitString]));

  // PEM encode
  const b64 = cert.toString("base64");
  const lines: string[] = [];
  for (let i = 0; i < b64.length; i += 64) {
    lines.push(b64.substring(i, i + 64));
  }
  return `-----BEGIN CERTIFICATE-----\n${lines.join("\n")}\n-----END CERTIFICATE-----\n`;
}

function buildTbsCertificate(
  serial: Buffer,
  subject: Buffer,
  issuer: Buffer,
  notBefore: Date,
  notAfter: Date,
  pubKeyDer: Buffer,
): Buffer {
  // Version: v3 (explicit tag [0])
  const version = Buffer.from([0xa0, 0x03, 0x02, 0x01, 0x02]);

  // Serial number
  const serialInt = derInteger(serial);

  // Signature algorithm: ecdsa-with-SHA256
  const sigAlg = derSequence(
    Buffer.from([0x06, 0x08, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x02]),
  );

  // Validity
  const validity = derSequence(
    Buffer.concat([derUtcTime(notBefore), derUtcTime(notAfter)]),
  );

  // Subject Alt Names extension (localhost, 127.0.0.1)
  const sanExt = buildSanExtension();

  // Extensions wrapper [3]
  const extensions = Buffer.concat([
    Buffer.from([0xa3]),
    derLengthBytes(derSequence(sanExt).length),
    derSequence(sanExt),
  ]);

  return derSequence(
    Buffer.concat([
      version,
      serialInt,
      sigAlg,
      issuer,
      validity,
      subject,
      pubKeyDer,
      extensions,
    ]),
  );
}

function buildName(cn: string): Buffer {
  // RDNSequence with CN and O
  const cnOid = Buffer.from([0x06, 0x03, 0x55, 0x04, 0x03]); // id-at-commonName
  const cnVal = derUtf8String(cn);
  const cnAttr = derSequence(Buffer.concat([cnOid, cnVal]));
  const cnRdn = derSet(cnAttr);

  const oOid = Buffer.from([0x06, 0x03, 0x55, 0x04, 0x0a]); // id-at-organizationName
  const oVal = derUtf8String("Wire");
  const oAttr = derSequence(Buffer.concat([oOid, oVal]));
  const oRdn = derSet(oAttr);

  return derSequence(Buffer.concat([cnRdn, oRdn]));
}

function buildSanExtension(): Buffer {
  // SubjectAltName OID: 2.5.29.17
  const sanOid = Buffer.from([0x06, 0x03, 0x55, 0x1d, 0x11]);

  // DNS: localhost
  const dns = Buffer.from("localhost", "utf-8");
  const dnsName = Buffer.concat([Buffer.from([0x82]), derLengthBytes(dns.length), dns]);

  // IP: 127.0.0.1
  const ip = Buffer.from([0x87, 0x04, 0x7f, 0x00, 0x00, 0x01]);

  const sanValue = derSequence(Buffer.concat([dnsName, ip]));
  const sanOctetString = derOctetString(sanValue);

  return derSequence(Buffer.concat([sanOid, sanOctetString]));
}

// ── ASN.1 DER helpers ───────────────────────────────────────────────────────

function derLengthBytes(len: number): Buffer {
  if (len < 0x80) return Buffer.from([len]);
  if (len < 0x100) return Buffer.from([0x81, len]);
  return Buffer.from([0x82, (len >> 8) & 0xff, len & 0xff]);
}

function derWrap(tag: number, content: Buffer): Buffer {
  const lenBytes = derLengthBytes(content.length);
  return Buffer.concat([Buffer.from([tag]), lenBytes, content]);
}

function derSequence(content: Buffer): Buffer {
  return derWrap(0x30, content);
}

function derSet(content: Buffer): Buffer {
  return derWrap(0x31, content);
}

function derInteger(value: Buffer): Buffer {
  // Ensure positive (add leading zero if high bit set)
  if (value[0] & 0x80) {
    return derWrap(0x02, Buffer.concat([Buffer.from([0x00]), value]));
  }
  return derWrap(0x02, value);
}

function derBitString(content: Buffer): Buffer {
  // Bit string with 0 unused bits
  return derWrap(0x03, Buffer.concat([Buffer.from([0x00]), content]));
}

function derOctetString(content: Buffer): Buffer {
  return derWrap(0x04, content);
}

function derUtf8String(s: string): Buffer {
  return derWrap(0x0c, Buffer.from(s, "utf-8"));
}

function derUtcTime(date: Date): Buffer {
  const y = date.getUTCFullYear() % 100;
  const m = date.getUTCMonth() + 1;
  const d = date.getUTCDate();
  const h = date.getUTCHours();
  const min = date.getUTCMinutes();
  const sec = date.getUTCSeconds();
  const s = `${pad2(y)}${pad2(m)}${pad2(d)}${pad2(h)}${pad2(min)}${pad2(sec)}Z`;
  return derWrap(0x17, Buffer.from(s, "ascii"));
}

function pad2(n: number): string {
  return n.toString().padStart(2, "0");
}
