import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import {
  MessageType,
  Flags,
  HttpMethod,
  encodeFrame,
  decodeFrame,
  encodeFilePayload,
  decodeFilePayload,
  encodeRelayPayload,
  decodeRelayPayload,
  encodeHttpRequest,
  decodeHttpRequest,
  encodeHttpResponse,
  decodeHttpResponse,
  httpMethodFromStr,
  httpMethodToStr,
  HEADER_SIZE,
} from "../src/protocol";
import { randomUUID } from "crypto";

describe("Frame encoding", () => {
  it("JSON roundtrip", () => {
    const payload = Buffer.from('{"hello":"world"}');
    const frame = encodeFrame(MessageType.JSON, payload);
    const [header, decoded] = decodeFrame(frame);
    assert.equal(header.msgType, MessageType.JSON);
    assert.deepEqual(decoded, payload);
  });

  it("Binary roundtrip", () => {
    const payload = Buffer.alloc(2560);
    for (let i = 0; i < payload.length; i++) payload[i] = i % 256;
    const frame = encodeFrame(MessageType.BINARY, payload);
    const [header, decoded] = decodeFrame(frame);
    assert.equal(header.msgType, MessageType.BINARY);
    assert.deepEqual(decoded, payload);
  });

  it("Compressed roundtrip", () => {
    const payload = Buffer.alloc(10000, 0x41); // 10KB of 'A'
    const frame = encodeFrame(MessageType.BINARY, payload, undefined, Flags.NONE, true);
    assert.ok(frame.length < payload.length); // Should be smaller
    const [header, decoded] = decodeFrame(frame);
    assert.ok(header.flags & Flags.COMPRESSED);
    assert.deepEqual(decoded, payload);
  });

  it("Small payload not compressed", () => {
    const payload = Buffer.from("tiny");
    const frame = encodeFrame(MessageType.JSON, payload, undefined, Flags.NONE, true);
    const [header, decoded] = decodeFrame(frame);
    assert.equal(header.flags & Flags.COMPRESSED, 0);
    assert.deepEqual(decoded, payload);
  });

  it("Custom msg_id", () => {
    const mid = Buffer.from(randomUUID().replace(/-/g, ""), "hex");
    const frame = encodeFrame(MessageType.JSON, Buffer.from("{}"), mid);
    const [header] = decodeFrame(frame);
    assert.deepEqual(header.msgId, mid);
  });

  it("Stream flags preserved", () => {
    for (const flag of [Flags.STREAM_START, Flags.STREAM_CHUNK, Flags.STREAM_END]) {
      const frame = encodeFrame(MessageType.FILE, Buffer.from("data"), undefined, flag);
      const [header] = decodeFrame(frame);
      assert.ok(header.flags & flag);
    }
  });

  it("Bad magic raises", () => {
    const frame = encodeFrame(MessageType.JSON, Buffer.from("{}"));
    const bad = Buffer.from(frame);
    bad[0] = 0x00;
    bad[1] = 0x00;
    assert.throws(() => decodeFrame(bad), /Bad magic/);
  });

  it("Truncated frame raises", () => {
    assert.throws(() => decodeFrame(Buffer.alloc(10)), /Frame too short/);
  });

  it("Empty payload", () => {
    const frame = encodeFrame(MessageType.PING, Buffer.alloc(0));
    const [header, payload] = decodeFrame(frame);
    assert.equal(header.msgType, MessageType.PING);
    assert.equal(payload.length, 0);
  });
});

describe("File payload", () => {
  it("Roundtrip", () => {
    const data = Buffer.from([0x50, 0x4b, 0x03, 0x04, 0x00, 0x00]);
    const encoded = encodeFilePayload("test.zip", data);
    const [name, decoded] = decodeFilePayload(encoded);
    assert.equal(name, "test.zip");
    assert.deepEqual(decoded, data);
  });

  it("Checksum mismatch raises", () => {
    const data = Buffer.from("original data content");
    const encoded = encodeFilePayload("corrupted.zip", data);
    encoded[encoded.length - 1] ^= 0xff; // Corrupt last byte
    assert.throws(() => decodeFilePayload(encoded), /checksum mismatch/);
  });

  it("Empty data", () => {
    const encoded = encodeFilePayload("empty.bin", Buffer.alloc(0));
    const [name, decoded] = decodeFilePayload(encoded);
    assert.equal(name, "empty.bin");
    assert.equal(decoded.length, 0);
  });
});

describe("Relay payload", () => {
  it("Roundtrip", () => {
    const inner = Buffer.from('{"test":true}');
    const encoded = encodeRelayPayload("src_fp_abc", "dst_fp_xyz", MessageType.JSON, inner);
    const [src, dst, innerType, innerPayload] = decodeRelayPayload(encoded);
    assert.equal(src, "src_fp_abc");
    assert.equal(dst, "dst_fp_xyz");
    assert.equal(innerType, MessageType.JSON);
    assert.deepEqual(innerPayload, inner);
  });
});

describe("HTTP request payload", () => {
  it("Roundtrip", () => {
    const headers: [string, string][] = [
      ["Content-Type", "application/json"],
      ["X-Custom", "value"],
    ];
    const body = Buffer.from('{"name":"test"}');
    const encoded = encodeHttpRequest(HttpMethod.POST, "/api/users", "page=1&limit=10", headers, body);
    const [method, path, query, decHeaders, decBody] = decodeHttpRequest(encoded);
    assert.equal(method, HttpMethod.POST);
    assert.equal(path, "/api/users");
    assert.equal(query, "page=1&limit=10");
    assert.deepEqual(decHeaders, headers);
    assert.deepEqual(decBody, body);
  });

  it("Empty body", () => {
    const encoded = encodeHttpRequest(HttpMethod.GET, "/", "", [], Buffer.alloc(0));
    const [method, path, query, headers, body] = decodeHttpRequest(encoded);
    assert.equal(method, HttpMethod.GET);
    assert.equal(body.length, 0);
  });

  it("All methods", () => {
    for (let m = 0; m <= 6; m++) {
      const encoded = encodeHttpRequest(m as HttpMethod, "/test", "", [], Buffer.alloc(0));
      const [decoded] = decodeHttpRequest(encoded);
      assert.equal(decoded, m);
    }
  });
});

describe("HTTP response payload", () => {
  it("Roundtrip", () => {
    const headers: [string, string][] = [["Content-Type", "application/json"]];
    const body = Buffer.from('{"ok":true}');
    const encoded = encodeHttpResponse(200, headers, body);
    const [status, decHeaders, decBody] = decodeHttpResponse(encoded);
    assert.equal(status, 200);
    assert.deepEqual(decHeaders, headers);
    assert.deepEqual(decBody, body);
  });

  it("Empty response", () => {
    const encoded = encodeHttpResponse(204, [], Buffer.alloc(0));
    const [status, headers, body] = decodeHttpResponse(encoded);
    assert.equal(status, 204);
    assert.equal(body.length, 0);
  });
});

describe("HttpMethod helpers", () => {
  it("fromStr", () => {
    assert.equal(httpMethodFromStr("GET"), HttpMethod.GET);
    assert.equal(httpMethodFromStr("post"), HttpMethod.POST);
    assert.equal(httpMethodFromStr("Delete"), HttpMethod.DELETE);
  });

  it("toStr", () => {
    assert.equal(httpMethodToStr(HttpMethod.GET), "GET");
    assert.equal(httpMethodToStr(HttpMethod.POST), "POST");
  });
});

describe("File payload — extended", () => {
  it("Unicode filename", () => {
    const data = Buffer.from("unicode data");
    const encoded = encodeFilePayload("日本語ファイル.txt", data);
    const [name, decoded] = decodeFilePayload(encoded);
    assert.equal(name, "日本語ファイル.txt");
    assert.deepEqual(decoded, data);
  });

  it("Large filename (200 chars)", () => {
    const longName = "a".repeat(200) + ".bin";
    const data = Buffer.from("data");
    const encoded = encodeFilePayload(longName, data);
    const [name, decoded] = decodeFilePayload(encoded);
    assert.equal(name, longName);
    assert.deepEqual(decoded, data);
  });

  it("Checksum is embedded in payload", () => {
    const data = Buffer.from("test data for checksum");
    const encoded = encodeFilePayload("check.bin", data);
    const nameLen = encoded.readUInt16BE(0);
    const checksumStart = 2 + nameLen;
    const checksum = encoded.subarray(checksumStart, checksumStart + 32);
    assert.equal(checksum.length, 32);
    // Verify it's a valid SHA-256
    const crypto = require("crypto");
    const expected = crypto.createHash("sha256").update(data).digest();
    assert.deepEqual(checksum, expected);
  });
});

describe("Frame — truncated payload", () => {
  it("Payload truncated raises", () => {
    const payload = Buffer.from("hello world");
    const frame = encodeFrame(MessageType.JSON, payload);
    // Chop off some of the payload
    const truncated = frame.subarray(0, frame.length - 5);
    assert.throws(() => decodeFrame(truncated), /Payload truncated/);
  });
});
