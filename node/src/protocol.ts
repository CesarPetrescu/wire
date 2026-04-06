/**
 * Wire protocol — binary framing over a single WebSocket.
 *
 * Frame layout (all multi-byte integers are big-endian):
 * ┌──────────┬──────────┬──────────────┬───────────┬──────────┬─────────┐
 * │ magic(2) │ type(1)  │ msg_id(16)   │ flags(1)  │ len(4)   │ payload │
 * └──────────┴──────────┴──────────────┴───────────┴──────────┴─────────┘
 */

import { randomUUID } from "crypto";
import * as zlib from "zlib";
import * as crypto from "crypto";

export const MAGIC = 0xbe01;
export const HEADER_SIZE = 24;
export const STREAM_CHUNK_SIZE = 4 * 1024 * 1024; // 4 MiB
export const CHECKSUM_SIZE = 32;

export enum MessageType {
  JSON = 0x01,
  BINARY = 0x02,
  FILE = 0x03,
  IMAGE = 0x04,
  RELAY = 0x05,
  HTTP_REQUEST = 0x06,
  HTTP_RESPONSE = 0x07,
  AUTH = 0x10,
  AUTH_OK = 0x11,
  AUTH_FAIL = 0x12,
  PING = 0xff,
}

export enum Flags {
  NONE = 0,
  STREAM_START = 1 << 0,
  STREAM_CHUNK = 1 << 1,
  STREAM_END = 1 << 2,
  COMPRESSED = 1 << 3,
}

export enum HttpMethod {
  GET = 0,
  POST = 1,
  PUT = 2,
  DELETE = 3,
  PATCH = 4,
  HEAD = 5,
  OPTIONS = 6,
}

export function httpMethodFromStr(method: string): HttpMethod {
  switch (method.toUpperCase()) {
    case "GET": return HttpMethod.GET;
    case "POST": return HttpMethod.POST;
    case "PUT": return HttpMethod.PUT;
    case "DELETE": return HttpMethod.DELETE;
    case "PATCH": return HttpMethod.PATCH;
    case "HEAD": return HttpMethod.HEAD;
    case "OPTIONS": return HttpMethod.OPTIONS;
    default: throw new Error(`Unknown HTTP method: ${method}`);
  }
}

export function httpMethodToStr(method: HttpMethod): string {
  const names = ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"];
  return names[method] ?? "GET";
}

export interface FrameHeader {
  msgType: MessageType;
  msgId: Buffer;
  flags: number;
  payloadLen: number;
}

function uuidToBytes(): Buffer {
  const uuid = randomUUID().replace(/-/g, "");
  return Buffer.from(uuid, "hex");
}

export function encodeFrame(
  msgType: MessageType,
  payload: Buffer,
  msgId?: Buffer,
  flags: number = Flags.NONE,
  compress: boolean = false,
): Buffer {
  if (!msgId) {
    msgId = uuidToBytes();
  }
  let actualPayload = payload;
  let actualFlags = flags;

  if (compress && payload.length > 256) {
    actualPayload = zlib.deflateSync(payload, { level: 6 });
    actualFlags |= Flags.COMPRESSED;
  }

  const header = Buffer.alloc(HEADER_SIZE);
  header.writeUInt16BE(MAGIC, 0);
  header.writeUInt8(msgType, 2);
  msgId.copy(header, 3, 0, 16);
  header.writeUInt8(actualFlags, 19);
  header.writeUInt32BE(actualPayload.length, 20);

  return Buffer.concat([header, actualPayload]);
}

export function decodeFrame(data: Buffer): [FrameHeader, Buffer] {
  if (data.length < HEADER_SIZE) {
    throw new Error(`Frame too short: ${data.length} < ${HEADER_SIZE}`);
  }

  const magic = data.readUInt16BE(0);
  if (magic !== MAGIC) {
    throw new Error(`Bad magic: 0x${magic.toString(16).padStart(4, "0")}`);
  }

  const msgType = data.readUInt8(2) as MessageType;
  const msgId = Buffer.alloc(16);
  data.copy(msgId, 0, 3, 19);
  const flags = data.readUInt8(19);
  const payloadLen = data.readUInt32BE(20);

  if (data.length < HEADER_SIZE + payloadLen) {
    throw new Error(
      `Payload truncated: got ${data.length - HEADER_SIZE}, expected ${payloadLen}`,
    );
  }

  let payload = data.subarray(HEADER_SIZE, HEADER_SIZE + payloadLen);

  if (flags & Flags.COMPRESSED) {
    payload = zlib.inflateSync(payload);
  }

  return [
    { msgType, msgId, flags, payloadLen: payload.length },
    Buffer.from(payload),
  ];
}

// ── File payload ────────────────────────────────────────────────────────────

export function encodeFilePayload(filename: string, data: Buffer): Buffer {
  const nameBytes = Buffer.from(filename, "utf-8");
  const checksum = crypto.createHash("sha256").update(data).digest();
  const buf = Buffer.alloc(2 + nameBytes.length + CHECKSUM_SIZE + data.length);
  buf.writeUInt16BE(nameBytes.length, 0);
  nameBytes.copy(buf, 2);
  checksum.copy(buf, 2 + nameBytes.length);
  data.copy(buf, 2 + nameBytes.length + CHECKSUM_SIZE);
  return buf;
}

export function decodeFilePayload(payload: Buffer): [string, Buffer] {
  const nameLen = payload.readUInt16BE(0);
  const filename = payload.subarray(2, 2 + nameLen).toString("utf-8");
  const checksumStart = 2 + nameLen;
  const expectedChecksum = payload.subarray(checksumStart, checksumStart + CHECKSUM_SIZE);
  const data = payload.subarray(checksumStart + CHECKSUM_SIZE);
  const actualChecksum = crypto.createHash("sha256").update(data).digest();
  if (!actualChecksum.equals(expectedChecksum)) {
    throw new Error(
      `File '${filename}' checksum mismatch: expected ${expectedChecksum.toString("hex")}, got ${actualChecksum.toString("hex")}`,
    );
  }
  return [filename, Buffer.from(data)];
}

// ── Relay payload ───────────────────────────────────────────────────────────

export function encodeRelayPayload(
  sourceFp: string,
  destFp: string,
  innerMsgType: MessageType,
  innerPayload: Buffer,
): Buffer {
  const src = Buffer.from(sourceFp, "utf-8");
  const dst = Buffer.from(destFp, "utf-8");
  const buf = Buffer.alloc(2 + src.length + 2 + dst.length + 1 + innerPayload.length);
  let offset = 0;
  buf.writeUInt16BE(src.length, offset); offset += 2;
  src.copy(buf, offset); offset += src.length;
  buf.writeUInt16BE(dst.length, offset); offset += 2;
  dst.copy(buf, offset); offset += dst.length;
  buf.writeUInt8(innerMsgType, offset); offset += 1;
  innerPayload.copy(buf, offset);
  return buf;
}

export function decodeRelayPayload(
  payload: Buffer,
): [string, string, MessageType, Buffer] {
  let offset = 0;
  const srcLen = payload.readUInt16BE(offset); offset += 2;
  const sourceFp = payload.subarray(offset, offset + srcLen).toString("utf-8"); offset += srcLen;
  const dstLen = payload.readUInt16BE(offset); offset += 2;
  const destFp = payload.subarray(offset, offset + dstLen).toString("utf-8"); offset += dstLen;
  const innerMsgType = payload.readUInt8(offset) as MessageType; offset += 1;
  const innerPayload = Buffer.from(payload.subarray(offset));
  return [sourceFp, destFp, innerMsgType, innerPayload];
}

// ── HTTP tunnel payloads ────────────────────────────────────────────────────

export function encodeHttpRequest(
  method: HttpMethod,
  path: string,
  query: string,
  headers: [string, string][],
  body: Buffer,
): Buffer {
  const pathBuf = Buffer.from(path, "utf-8");
  const queryBuf = Buffer.from(query, "utf-8");
  const parts: Buffer[] = [];

  // Method
  const methodBuf = Buffer.alloc(1);
  methodBuf.writeUInt8(method, 0);
  parts.push(methodBuf);

  // Path
  const pathLenBuf = Buffer.alloc(2);
  pathLenBuf.writeUInt16BE(pathBuf.length, 0);
  parts.push(pathLenBuf, pathBuf);

  // Query
  const queryLenBuf = Buffer.alloc(2);
  queryLenBuf.writeUInt16BE(queryBuf.length, 0);
  parts.push(queryLenBuf, queryBuf);

  // Headers
  const hdrCountBuf = Buffer.alloc(2);
  hdrCountBuf.writeUInt16BE(headers.length, 0);
  parts.push(hdrCountBuf);
  for (const [key, val] of headers) {
    const kb = Buffer.from(key, "utf-8");
    const vb = Buffer.from(val, "utf-8");
    const klBuf = Buffer.alloc(2);
    klBuf.writeUInt16BE(kb.length, 0);
    const vlBuf = Buffer.alloc(2);
    vlBuf.writeUInt16BE(vb.length, 0);
    parts.push(klBuf, kb, vlBuf, vb);
  }

  // Body
  const bodyLenBuf = Buffer.alloc(4);
  bodyLenBuf.writeUInt32BE(body.length, 0);
  parts.push(bodyLenBuf, body);

  return Buffer.concat(parts);
}

export function decodeHttpRequest(
  payload: Buffer,
): [HttpMethod, string, string, [string, string][], Buffer] {
  let offset = 0;
  const method = payload.readUInt8(offset) as HttpMethod; offset += 1;
  const pathLen = payload.readUInt16BE(offset); offset += 2;
  const path = payload.subarray(offset, offset + pathLen).toString("utf-8"); offset += pathLen;
  const queryLen = payload.readUInt16BE(offset); offset += 2;
  const query = payload.subarray(offset, offset + queryLen).toString("utf-8"); offset += queryLen;
  const headerCount = payload.readUInt16BE(offset); offset += 2;
  const headers: [string, string][] = [];
  for (let i = 0; i < headerCount; i++) {
    const kl = payload.readUInt16BE(offset); offset += 2;
    const key = payload.subarray(offset, offset + kl).toString("utf-8"); offset += kl;
    const vl = payload.readUInt16BE(offset); offset += 2;
    const val = payload.subarray(offset, offset + vl).toString("utf-8"); offset += vl;
    headers.push([key, val]);
  }
  const bodyLen = payload.readUInt32BE(offset); offset += 4;
  const body = Buffer.from(payload.subarray(offset, offset + bodyLen));
  return [method, path, query, headers, body];
}

export function encodeHttpResponse(
  statusCode: number,
  headers: [string, string][],
  body: Buffer,
): Buffer {
  const parts: Buffer[] = [];
  const statusBuf = Buffer.alloc(2);
  statusBuf.writeUInt16BE(statusCode, 0);
  parts.push(statusBuf);

  const hdrCountBuf = Buffer.alloc(2);
  hdrCountBuf.writeUInt16BE(headers.length, 0);
  parts.push(hdrCountBuf);
  for (const [key, val] of headers) {
    const kb = Buffer.from(key, "utf-8");
    const vb = Buffer.from(val, "utf-8");
    const klBuf = Buffer.alloc(2);
    klBuf.writeUInt16BE(kb.length, 0);
    const vlBuf = Buffer.alloc(2);
    vlBuf.writeUInt16BE(vb.length, 0);
    parts.push(klBuf, kb, vlBuf, vb);
  }

  const bodyLenBuf = Buffer.alloc(4);
  bodyLenBuf.writeUInt32BE(body.length, 0);
  parts.push(bodyLenBuf, body);

  return Buffer.concat(parts);
}

export function decodeHttpResponse(
  payload: Buffer,
): [number, [string, string][], Buffer] {
  let offset = 0;
  const statusCode = payload.readUInt16BE(offset); offset += 2;
  const headerCount = payload.readUInt16BE(offset); offset += 2;
  const headers: [string, string][] = [];
  for (let i = 0; i < headerCount; i++) {
    const kl = payload.readUInt16BE(offset); offset += 2;
    const key = payload.subarray(offset, offset + kl).toString("utf-8"); offset += kl;
    const vl = payload.readUInt16BE(offset); offset += 2;
    const val = payload.subarray(offset, offset + vl).toString("utf-8"); offset += vl;
    headers.push([key, val]);
  }
  const bodyLen = payload.readUInt32BE(offset); offset += 4;
  const body = Buffer.from(payload.subarray(offset, offset + bodyLen));
  return [statusCode, headers, body];
}
