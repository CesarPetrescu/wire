#!/usr/bin/env node
/**
 * Wire Node.js CLI — SubController mode.
 *
 * Usage:
 *   wire-node sub --host 127.0.0.1 --port 8765 --secret mysecret
 */

import { SubController, ServiceDef } from "./subcontroller";

async function main() {
  const args = process.argv.slice(2);
  if (args.length < 1) {
    console.error("Usage: wire-node sub --host H --port P --secret S");
    process.exit(1);
  }

  const command = args[0];
  if (command !== "sub") {
    console.error(`Unknown command: ${command}. Only 'sub' is supported.`);
    process.exit(1);
  }

  let host = "127.0.0.1";
  let port = 8765;
  let secret = "changeme";

  for (let i = 1; i < args.length; i++) {
    if (args[i] === "--host" && args[i + 1]) { host = args[++i]; }
    else if (args[i] === "--port" && args[i + 1]) { port = parseInt(args[++i], 10); }
    else if (args[i] === "--secret" && args[i + 1]) { secret = args[++i]; }
  }

  const sub = new SubController({
    controllerUrl: `wss://${host}:${port}`,
    presharedSecret: secret,
  });

  sub.on("json", (data: any) => {
    console.log(`[JSON from controller]: ${JSON.stringify(data)}`);
  });

  sub.on("binary", (data: Buffer) => {
    console.log(`[BINARY from controller]: ${data.length} bytes`);
  });

  sub.on("file", (filename: string, data: Buffer) => {
    console.log(`[FILE from controller]: ${filename} (${data.length} bytes)`);
  });

  sub.on("image", (filename: string, data: Buffer) => {
    console.log(`[IMAGE from controller]: ${filename} (${data.length} bytes)`);
  });

  sub.on("relay:json", (sourceFp: string, data: any) => {
    console.log(`[RELAY JSON from ${sourceFp.substring(0, 16)}...]: ${JSON.stringify(data)}`);
  });

  sub.on("relay:binary", (sourceFp: string, data: Buffer) => {
    console.log(`[RELAY BINARY from ${sourceFp.substring(0, 16)}...]: ${data.length} bytes`);
  });

  sub.on("relay:file", (sourceFp: string, filename: string, data: Buffer) => {
    console.log(`[RELAY FILE from ${sourceFp.substring(0, 16)}...]: ${filename} (${data.length} bytes)`);
  });

  sub.on("peer:joined", (peerFp: string) => {
    console.log(`[PEER JOINED]: ${peerFp}`);
  });

  sub.on("peer:left", (peerFp: string) => {
    console.log(`[PEER LEFT]: ${peerFp}`);
  });

  try {
    await sub.connect();
    console.log("SubController connected. Sending test JSON...");
    await sub.sendJson({ hello: "from js subcontroller" });

    // Keep running until SIGTERM/SIGINT
    await new Promise<void>((resolve) => {
      process.on("SIGTERM", () => {
        sub.disconnect().finally(resolve);
      });
      process.on("SIGINT", () => {
        sub.disconnect().finally(resolve);
      });
    });
  } catch (err) {
    console.error("Error:", err);
    process.exit(1);
  }

  process.exit(0);
}

main();
