import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import { generateSelfSignedCert, getCertFingerprint } from "../src/certs";

describe("Certificate generation", () => {
  it("generates valid PEM cert and key", () => {
    const bundle = generateSelfSignedCert("test-node");
    assert.ok(bundle.certPem.includes("BEGIN CERTIFICATE"));
    assert.ok(bundle.keyPem.includes("BEGIN PRIVATE KEY") || bundle.keyPem.includes("BEGIN EC PRIVATE KEY"));
    assert.equal(bundle.fingerprint.length, 64); // SHA-256 hex
  });

  it("fingerprint is stable", () => {
    const bundle = generateSelfSignedCert("stable");
    const fp1 = getCertFingerprint(bundle.certPem);
    const fp2 = getCertFingerprint(bundle.certPem);
    assert.equal(fp1, fp2);
    assert.equal(fp1, bundle.fingerprint);
  });

  it("different certs have different fingerprints", () => {
    const a = generateSelfSignedCert("a");
    const b = generateSelfSignedCert("b");
    assert.notEqual(a.fingerprint, b.fingerprint);
  });

  it("fingerprint is 64 hex chars", () => {
    const bundle = generateSelfSignedCert("hex-test");
    assert.match(bundle.fingerprint, /^[0-9a-f]{64}$/);
  });
});
