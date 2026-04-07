import { describe, it, afterEach } from "node:test";
import * as assert from "node:assert/strict";
import * as crypto from "crypto";
import { Controller } from "../src/controller";
import { SubController } from "../src/subcontroller";
import { MessageType } from "../src/protocol";

const SECRET = "integration-test-secret-42";
let _port = 26000;
function nextPort(): number { return _port++; }

async function makePair(port?: number): Promise<[Controller, SubController]> {
  const p = port ?? nextPort();
  const ctrl = new Controller("127.0.0.1", p, SECRET);
  await ctrl.start();
  const sub = new SubController({
    controllerUrl: `wss://127.0.0.1:${p}`,
    presharedSecret: SECRET,
  });
  await sub.connect();
  return [ctrl, sub];
}

// Cleanup helper
let cleanups: (() => Promise<void>)[] = [];
afterEach(async () => {
  for (const fn of cleanups) await fn();
  cleanups = [];
});

function trackCleanup(ctrl: Controller, sub: SubController) {
  cleanups.push(async () => {
    await sub.disconnect();
    await ctrl.stop();
  });
}

describe("Auth", () => {
  it("success", async () => {
    const [ctrl, sub] = await makePair();
    trackCleanup(ctrl, sub);
    assert.ok(sub.fingerprint);
    assert.equal(ctrl.peerFingerprints.length, 1);
    assert.equal(ctrl.peerFingerprints[0], sub.fingerprint);
  });

  it("bad secret", async () => {
    const port = nextPort();
    const ctrl = new Controller("127.0.0.1", port, SECRET);
    await ctrl.start();
    cleanups.push(async () => { await ctrl.stop(); });

    const sub = new SubController({
      controllerUrl: `wss://127.0.0.1:${port}`,
      presharedSecret: "wrong-secret",
    });
    await assert.rejects(sub.connect(), /Authentication failed/);
  });
});

describe("JSON messaging", () => {
  it("sub to controller", async () => {
    const [ctrl, sub] = await makePair();
    trackCleanup(ctrl, sub);

    const received = new Promise<any>((resolve) => {
      ctrl.onMessage(MessageType.JSON, (_fp: string, data: any) => resolve(data));
    });
    await sub.sendJson({ hello: "world" });
    const data = await received;
    assert.deepEqual(data, { hello: "world" });
  });

  it("controller to sub", async () => {
    const [ctrl, sub] = await makePair();
    trackCleanup(ctrl, sub);

    const received = new Promise<any>((resolve) => {
      sub.on("json", (data: any) => resolve(data));
    });
    await ctrl.sendJson(sub.fingerprint!, { from: "controller", n: 7 });
    const data = await received;
    assert.deepEqual(data, { from: "controller", n: 7 });
  });

  it("bidirectional", async () => {
    const [ctrl, sub] = await makePair();
    trackCleanup(ctrl, sub);

    const ctrlReceived = new Promise<any>((resolve) => {
      ctrl.onMessage(MessageType.JSON, (_fp: string, data: any) => resolve(data));
    });
    const subReceived = new Promise<any>((resolve) => {
      sub.on("json", (data: any) => resolve(data));
    });

    await sub.sendJson({ direction: "up" });
    await ctrl.sendJson(sub.fingerprint!, { direction: "down" });

    assert.deepEqual(await ctrlReceived, { direction: "up" });
    assert.deepEqual(await subReceived, { direction: "down" });
  });
});

describe("Binary data", () => {
  it("sub to controller", async () => {
    const [ctrl, sub] = await makePair();
    trackCleanup(ctrl, sub);

    const payload = crypto.randomBytes(4096);
    const received = new Promise<Buffer>((resolve) => {
      ctrl.onMessage(MessageType.BINARY, (_fp: string, data: Buffer) => resolve(data));
    });
    await sub.sendBinary(payload);
    const data = await received;
    assert.deepEqual(data, payload);
  });
});

describe("File transfer", () => {
  it("small zip", async () => {
    const [ctrl, sub] = await makePair();
    trackCleanup(ctrl, sub);

    const fileData = crypto.randomBytes(1024);
    const received = new Promise<[string, Buffer]>((resolve) => {
      ctrl.onMessage(MessageType.FILE, (_fp: string, name: string, data: Buffer) => resolve([name, data]));
    });
    await sub.sendFile("test.zip", fileData);
    const [name, data] = await received;
    assert.equal(name, "test.zip");
    assert.deepEqual(data, fileData);
  });

  it("image", async () => {
    const [ctrl, sub] = await makePair();
    trackCleanup(ctrl, sub);

    const imgData = crypto.randomBytes(2048);
    const received = new Promise<[string, Buffer]>((resolve) => {
      ctrl.onMessage(MessageType.IMAGE, (_fp: string, name: string, data: Buffer) => resolve([name, data]));
    });
    await sub.sendFile("photo.png", imgData, true);
    const [name, data] = await received;
    assert.equal(name, "photo.png");
    assert.deepEqual(data, imgData);
  });
});

describe("Multiple subs", () => {
  it("rapid fire JSON", async () => {
    const port = nextPort();
    const ctrl = new Controller("127.0.0.1", port, SECRET);
    await ctrl.start();

    const sub = new SubController({
      controllerUrl: `wss://127.0.0.1:${port}`,
      presharedSecret: SECRET,
    });
    await sub.connect();

    cleanups.push(async () => {
      await sub.disconnect();
      await ctrl.stop();
    });

    let count = 0;
    const allReceived = new Promise<void>((resolve) => {
      ctrl.onMessage(MessageType.JSON, () => {
        count++;
        if (count >= 10) resolve();
      });
    });

    for (let i = 0; i < 10; i++) {
      await sub.sendJson({ index: i });
    }
    await allReceived;
    assert.equal(count, 10);
  });

  it("interleaved types", async () => {
    const [ctrl, sub] = await makePair();
    trackCleanup(ctrl, sub);

    const messages: string[] = [];
    const done = new Promise<void>((resolve) => {
      ctrl.onMessage(MessageType.JSON, () => { messages.push("json"); if (messages.length >= 3) resolve(); });
      ctrl.onMessage(MessageType.BINARY, () => { messages.push("binary"); if (messages.length >= 3) resolve(); });
      ctrl.onMessage(MessageType.FILE, () => { messages.push("file"); if (messages.length >= 3) resolve(); });
    });

    await sub.sendJson({ test: true });
    await sub.sendBinary(Buffer.from("binary data"));
    await sub.sendFile("f.txt", Buffer.from("file content"));

    await done;
    assert.equal(messages.length, 3);
    assert.ok(messages.includes("json"));
    assert.ok(messages.includes("binary"));
    assert.ok(messages.includes("file"));
  });
});
