import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import net from 'node:net';
import process from 'node:process';

const serverUrl = process.env.OXIDESFU_URL ?? process.env.LIVEKIT_URL;
const apiKey = process.env.OXIDESFU_API_KEY ?? process.env.LIVEKIT_API_KEY;
const apiSecret = process.env.OXIDESFU_API_SECRET ?? process.env.LIVEKIT_API_SECRET;

if (!serverUrl || !apiKey || !apiSecret) {
  console.error('Missing required env: OXIDESFU_URL/OXIDESFU_API_KEY/OXIDESFU_API_SECRET or LIVEKIT_URL/LIVEKIT_API_KEY/LIVEKIT_API_SECRET');
  process.exit(1);
}

const signal = new URL(serverUrl);
if (!['ws:', 'wss:', 'http:', 'https:'].includes(signal.protocol)) {
  console.error(`Unsupported OXIDESFU_URL protocol: ${signal.protocol}`);
  process.exit(1);
}

const host = signal.hostname;
const port = Number(signal.port || (signal.protocol === 'wss:' || signal.protocol === 'https:' ? 443 : 80));
const bindAddress = `${host}:${port}`;

const scriptPath = fileURLToPath(import.meta.url);
const scriptDir = path.dirname(scriptPath);
const browserRoot = path.resolve(scriptDir, '..');
const workspaceRoot = path.resolve(scriptDir, '../../../..');

let spawnedServer = null;

function canConnect(hostname, portNumber, timeoutMs = 800) {
  return new Promise((resolve) => {
    const socket = new net.Socket();
    let done = false;

    const finish = (value) => {
      if (done) return;
      done = true;
      socket.destroy();
      resolve(value);
    };

    socket.setTimeout(timeoutMs);
    socket.once('connect', () => finish(true));
    socket.once('timeout', () => finish(false));
    socket.once('error', () => finish(false));
    socket.connect(portNumber, hostname);
  });
}

async function waitForPort(hostname, portNumber, timeoutMs = 90_000, probeEveryMs = 300) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await canConnect(hostname, portNumber)) {
      return true;
    }
    await new Promise((r) => setTimeout(r, probeEveryMs));
  }
  return false;
}

function spawnOxideServer() {
  console.log(`[oxidesfu-test] starting oxidesfu-server on ${bindAddress}`);
  return spawn(
    'cargo',
    ['run', '-p', 'oxidesfu-server', '--', '--bind', bindAddress, '--api-key', apiKey, '--api-secret', apiSecret],
    {
      cwd: workspaceRoot,
      stdio: 'inherit',
      env: {
        ...process.env,
        RUST_LOG: process.env.RUST_LOG || 'oxidesfu_signaling=info,oxidesfu_server=info',
      },
    },
  );
}

function runPlaywright() {
  return new Promise((resolve) => {
    const child = spawn('npx', ['playwright', 'test', '--project=firefox', ...process.argv.slice(2)], {
      cwd: browserRoot,
      stdio: 'inherit',
      env: process.env,
    });

    child.once('exit', (code, signalName) => {
      if (signalName) {
        console.error(`[oxidesfu-test] playwright exited via signal ${signalName}`);
        resolve(1);
        return;
      }
      resolve(code ?? 1);
    });
  });
}

function stopSpawnedServer() {
  if (!spawnedServer || spawnedServer.killed) return;
  console.log('[oxidesfu-test] stopping spawned oxidesfu-server');
  spawnedServer.kill('SIGTERM');
}

process.on('SIGINT', () => {
  stopSpawnedServer();
  process.exit(130);
});
process.on('SIGTERM', () => {
  stopSpawnedServer();
  process.exit(143);
});

const autostart = process.env.OXIDESFU_AUTOSTART !== '0';

if (!(await canConnect(host, port))) {
  if (!autostart) {
    console.error(`[oxidesfu-test] OxideSFU is not reachable at ${host}:${port}. Start it manually or set OXIDESFU_AUTOSTART=1.`);
    process.exit(1);
  }

  spawnedServer = spawnOxideServer();
  const ready = await waitForPort(host, port);
  if (!ready) {
    stopSpawnedServer();
    console.error(`[oxidesfu-test] Timed out waiting for oxidesfu-server on ${host}:${port}`);
    process.exit(1);
  }
}

const exitCode = await runPlaywright();
stopSpawnedServer();
process.exit(exitCode);
