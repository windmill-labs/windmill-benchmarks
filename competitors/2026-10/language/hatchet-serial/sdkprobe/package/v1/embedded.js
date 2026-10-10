"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __asyncValues = (this && this.__asyncValues) || function (o) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var m = o[Symbol.asyncIterator], i;
    return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i);
    function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }
    function settle(resolve, reject, d, v) { Promise.resolve(v).then(function(v) { resolve({ value: v, done: d }); }, reject); }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.HatchetEmbeddedClient = void 0;
exports.stopEmbeddedSidecar = stopEmbeddedSidecar;
exports.startEmbeddedSidecar = startEmbeddedSidecar;
/**
 * Embedded mode runs a full local Hatchet engine as a sidecar process managed by your
 * application. It needs no API token, no Docker, and no external services (by default
 * the sidecar starts a bundled Postgres), which makes it a good fit for local
 * development and CI.
 *
 * ```typescript
 * import { HatchetEmbeddedClient } from './embedded.js';
 *
 * const hatchet = await HatchetEmbeddedClient.init();
 * // ... register workers and run tasks as usual ...
 * await hatchet.stopEmbedded();
 * ```
 *
 * See the [embedded mode guide](https://docs.hatchet.run/v1/embedded) for setup and
 * configuration details.
 *
 * @module Embedded
 */
const child_process_1 = require("child_process");
const client_1 = require("./client/client");
const crypto_1 = require("crypto");
const fs_1 = require("fs");
const fs = __importStar(require("fs/promises"));
const os = __importStar(require("os"));
const path = __importStar(require("path"));
const stream_1 = require("stream");
const net_1 = require("net");
const promises_1 = require("stream/promises");
const REPO_URL = 'https://github.com/hatchet-dev/hatchet-embedded';
const DEFAULT_READY_TIMEOUT_MS = 300000;
const SLOW_NOTICE_DELAY_MS = 2000;
const HEARTBEAT_INTERVAL_MS = 30000;
// first-contact progress goes to stderr (like the engine's own output) so it
// never corrupts program output; warm starts print at most one line
function logProgress(message) {
    process.stderr.write(`hatchet embedded: ${message}\n`);
}
// prints startMsg only if fn is still running after SLOW_NOTICE_DELAY_MS (and
// doneMsg once it finishes), so fast warm-start network calls stay quiet while
// a blocked one explains what the process is waiting on
function withSlowNotice(startMsg, doneMsg, fn) {
    return __awaiter(this, void 0, void 0, function* () {
        var _a;
        let noticed = false;
        let finished = false;
        const timer = setTimeout(() => {
            // guards a callback already queued when fn settles, so a stale start
            // message can never print after completion
            if (finished) {
                return;
            }
            noticed = true;
            logProgress(startMsg);
        }, SLOW_NOTICE_DELAY_MS);
        (_a = timer.unref) === null || _a === void 0 ? void 0 : _a.call(timer);
        let result;
        try {
            result = yield fn();
        }
        finally {
            finished = true;
            clearTimeout(timer);
        }
        // only reached when fn succeeded
        if (noticed) {
            logProgress(doneMsg);
        }
        return result;
    });
}
function sidecarAssetName() {
    const platform = { darwin: 'darwin', linux: 'linux' }[process.platform];
    const arch = { x64: 'amd64', arm64: 'arm64' }[process.arch];
    if (!platform || !arch) {
        throw new Error(`hatchet embedded is not supported on ${process.platform}/${process.arch}`);
    }
    return `hatchet-embedded-sidecar_${platform}_${arch}`;
}
function resolveVersion(version) {
    return __awaiter(this, void 0, void 0, function* () {
        var _a;
        const requested = (_a = version !== null && version !== void 0 ? version : process.env.HATCHET_CLIENT_EMBEDDED_VERSION) !== null && _a !== void 0 ? _a : 'latest';
        if (requested !== 'latest') {
            return requested;
        }
        const res = yield fetch(`${REPO_URL}/releases/latest`, { redirect: 'manual' });
        const location = res.headers.get('location');
        const tag = location === null || location === void 0 ? void 0 : location.split('/').pop();
        if (!tag || !tag.startsWith('v')) {
            throw new Error(`could not resolve the latest hatchet-embedded release from ${REPO_URL}`);
        }
        return tag;
    });
}
function expectedChecksum(tag, asset) {
    return __awaiter(this, void 0, void 0, function* () {
        const url = `${REPO_URL}/releases/download/${tag}/checksums.txt`;
        const res = yield fetch(url, { signal: AbortSignal.timeout(10000) });
        if (!res.ok) {
            throw new Error(`could not download ${url}: ${res.status}`);
        }
        for (const line of (yield res.text()).split('\n')) {
            const parts = line.split(/\s+/).filter(Boolean);
            if (parts.length === 2 && parts[1] === asset) {
                return parts[0];
            }
        }
        throw new Error(`no checksum for ${asset} in ${url}`);
    });
}
function sha256File(filePath) {
    return __awaiter(this, void 0, void 0, function* () {
        var _a, e_1, _b, _c;
        const digest = (0, crypto_1.createHash)('sha256');
        try {
            for (var _d = true, _e = __asyncValues((0, fs_1.createReadStream)(filePath)), _f; _f = yield _e.next(), _a = _f.done, !_a; _d = true) {
                _c = _f.value;
                _d = false;
                const chunk = _c;
                digest.update(chunk);
            }
        }
        catch (e_1_1) { e_1 = { error: e_1_1 }; }
        finally {
            try {
                if (!_d && !_a && (_b = _e.return)) yield _b.call(_e);
            }
            finally { if (e_1) throw e_1.error; }
        }
        return digest.digest('hex');
    });
}
function resolveExpectedChecksum(tag, asset, binPath) {
    return __awaiter(this, void 0, void 0, function* () {
        const checksumPath = path.join(path.dirname(binPath), `${asset}.sha256`);
        // fall back to the checksum cached at download time so a pinned, already
        // verified binary still starts when GitHub is unreachable
        let expected;
        try {
            expected = yield expectedChecksum(tag, asset);
        }
        catch (err) {
            const cachedDigest = yield fs.readFile(checksumPath, 'utf8').then((s) => s.trim(), () => undefined);
            const hasBinary = yield fs.access(binPath).then(() => true, () => false);
            if (cachedDigest && hasBinary) {
                return cachedDigest;
            }
            throw err;
        }
        yield fs.writeFile(checksumPath, `${expected}\n`);
        return expected;
    });
}
function ensureSidecarBinary(version, checksum) {
    return __awaiter(this, void 0, void 0, function* () {
        const tag = yield withSlowNotice(`resolving the latest hatchet-embedded release from ${REPO_URL}`, 'resolved the latest hatchet-embedded release', () => resolveVersion(version));
        const asset = sidecarAssetName();
        const binPath = path.join(os.homedir(), '.hatchet', 'embedded', tag, asset);
        yield fs.mkdir(path.dirname(binPath), { recursive: true });
        // verified on every start, not just at download; a cached binary that no
        // longer matches the expected checksum is re-downloaded
        const expected = checksum !== null && checksum !== void 0 ? checksum : (yield withSlowNotice(`resolving the expected checksum for ${tag} (the cached checksum is used if the release cannot be reached)`, 'expected checksum resolved', () => resolveExpectedChecksum(tag, asset, binPath)));
        const cached = yield fs.access(binPath).then(() => true, () => false);
        if (cached && (yield sha256File(binPath)) === expected) {
            return binPath;
        }
        logProgress(`downloading the embedded engine sidecar ${tag} to ${binPath} (tens of MB, cached for later runs)`);
        const url = `${REPO_URL}/releases/download/${tag}/${asset}`;
        const res = yield fetch(url);
        if (!res.ok || !res.body) {
            throw new Error(`could not download the hatchet embedded sidecar from ${url}: ${res.status}`);
        }
        // unique temp name per call (not per process) so concurrent downloads of
        // the same version never clobber each other, even within one process; the
        // final rename is atomic and last-writer-wins
        const tmpPath = `${binPath}.${(0, crypto_1.randomUUID)()}.download`;
        try {
            yield (0, promises_1.pipeline)(stream_1.Readable.fromWeb(res.body), (0, fs_1.createWriteStream)(tmpPath, { mode: 0o755 }));
            const actual = yield sha256File(tmpPath);
            if (actual !== expected) {
                throw new Error(`checksum mismatch for ${url}: expected ${expected}, got ${actual}`);
            }
            yield fs.rename(tmpPath, binPath);
        }
        finally {
            yield fs.rm(tmpPath, { force: true });
        }
        logProgress(`sidecar ${tag} downloaded`);
        return binPath;
    });
}
function waitForHandshake(child, handshakePath, timeoutMs) {
    return __awaiter(this, void 0, void 0, function* () {
        const start = Date.now();
        const deadline = start + timeoutMs;
        let nextHeartbeat = start + HEARTBEAT_INTERVAL_MS;
        let exited;
        child.once('exit', (code) => {
            exited = new Error(`hatchet embedded sidecar exited with code ${code} before becoming ready`);
        });
        while (Date.now() < deadline) {
            if (exited) {
                throw exited;
            }
            if (Date.now() >= nextHeartbeat) {
                const elapsed = Math.round((Date.now() - start) / 1000);
                logProgress(`still waiting for the embedded engine (${elapsed}s elapsed)`);
                nextHeartbeat += HEARTBEAT_INTERVAL_MS;
            }
            try {
                const handshake = JSON.parse(yield fs.readFile(handshakePath, 'utf8'));
                if (handshake.token) {
                    return handshake;
                }
            }
            catch (_a) {
                // not ready yet
            }
            yield new Promise((resolve) => {
                setTimeout(resolve, 200);
            });
        }
        child.kill();
        throw new Error(`hatchet embedded sidecar did not become ready within ${timeoutMs}ms`);
    });
}
// sidecars started in this process that have not been stopped yet
const activeSidecars = new Set();
// the ambient-token warning is printed at most once per process
let warnedAmbientToken = false;
/**
 * Gracefully stops every sidecar started in this process by
 * `HatchetEmbeddedClient.init()` (or `startEmbeddedSidecar`) and resolves once
 * they have fully exited, including their bundled Postgres. Call this before
 * your process exits so the engine's shutdown output does not print after
 * your program has returned.
 */
function stopEmbeddedSidecar() {
    return __awaiter(this, void 0, void 0, function* () {
        for (const sidecar of [...activeSidecars]) {
            yield sidecar.stop();
        }
    });
}
/**
 * Downloads (and caches) the hatchet-embedded sidecar binary, spawns it, and waits
 * until the embedded engine is ready. The sidecar shuts down when this process exits.
 * Use {@link HatchetEmbeddedClient.init} unless you need the raw connection details.
 */
function startEmbeddedSidecar() {
    return __awaiter(this, arguments, void 0, function* (opts = {}) {
        var _a, _b;
        if (process.env.HATCHET_CLIENT_TOKEN && !warnedAmbientToken) {
            warnedAmbientToken = true;
            logProgress('warning: HATCHET_CLIENT_TOKEN is set in the environment. Hatchet clients created with the standard constructor in this process will NOT use the embedded engine; unset HATCHET_CLIENT_TOKEN for embedded runs.');
        }
        const suppliedPath = (_a = opts.binaryPath) !== null && _a !== void 0 ? _a : process.env.HATCHET_CLIENT_EMBEDDED_BINARY_PATH;
        let binPath;
        if (suppliedPath) {
            if (opts.checksum) {
                const actual = yield sha256File(suppliedPath);
                if (actual !== opts.checksum) {
                    throw new Error(`checksum mismatch for ${suppliedPath}: expected ${opts.checksum}, got ${actual}`);
                }
            }
            binPath = suppliedPath;
        }
        else {
            binPath = yield ensureSidecarBinary(opts.version, opts.checksum);
        }
        const handshakePath = path.join(yield fs.mkdtemp(path.join(os.tmpdir(), 'hatchet-embedded-')), 'handshake.json');
        const args = ['-handshake-file', handshakePath];
        if (opts.databaseUrl) {
            args.push('-database-url', opts.databaseUrl);
        }
        if (opts.rabbitmqUrl) {
            args.push('-rabbitmq-url', opts.rabbitmqUrl);
        }
        if (opts.postgresDataDir) {
            args.push('-postgres-data-dir', opts.postgresDataDir);
        }
        if (opts.grpcPort) {
            args.push('-grpc-port', String(opts.grpcPort));
        }
        if (opts.apiPort) {
            args.push('-api-port', String(opts.apiPort));
        }
        if (opts.startApi === false) {
            args.push('-no-api');
        }
        if (opts.runMigrations === false) {
            args.push('-no-migrations');
        }
        if (opts.logLevel) {
            args.push('-log-level', opts.logLevel);
        }
        // the sidecar shuts down when its stdin closes, so it never outlives this
        // process, no matter how this process dies
        const child = (0, child_process_1.spawn)(binPath, args, { stdio: ['pipe', 'ignore', 'inherit'] });
        const killChild = () => child.kill();
        process.once('exit', killChild);
        logProgress(opts.databaseUrl
            ? 'starting the embedded engine'
            : 'starting the embedded engine (first run initializes a bundled Postgres and can take a minute)');
        let handshake;
        try {
            handshake = yield waitForHandshake(child, handshakePath, (_b = opts.readyTimeoutMs) !== null && _b !== void 0 ? _b : DEFAULT_READY_TIMEOUT_MS);
        }
        finally {
            yield fs.rm(path.dirname(handshakePath), { recursive: true, force: true }).catch(() => { });
        }
        child.unref();
        if (child.stdin instanceof net_1.Socket) {
            child.stdin.unref();
        }
        let stopping;
        const sidecar = {
            token: handshake.token,
            tenantId: handshake.tenant_id,
            grpcAddress: handshake.grpc_address,
            apiUrl: handshake.api_url,
            stop: () => {
                stopping !== null && stopping !== void 0 ? stopping : (stopping = new Promise((resolve) => {
                    var _a;
                    activeSidecars.delete(sidecar);
                    process.removeListener('exit', killChild);
                    if (child.exitCode !== null) {
                        resolve();
                        return;
                    }
                    const forceKill = setTimeout(() => child.kill('SIGKILL'), 30000);
                    (_a = forceKill.unref) === null || _a === void 0 ? void 0 : _a.call(forceKill);
                    child.once('exit', () => {
                        clearTimeout(forceKill);
                        resolve();
                    });
                    child.kill();
                }));
                return stopping;
            },
        };
        activeSidecars.add(sidecar);
        return sidecar;
    });
}
/**
 * Entry point for embedded mode. `init()` starts a full local Hatchet engine and
 * returns a regular Hatchet client connected to it. No API token or Docker is needed.
 * See the [embedded mode guide](https://docs.hatchet.run/v1/embedded).
 */
class HatchetEmbeddedClient {
    /**
     * Runs a full Hatchet engine locally via the hatchet-embedded sidecar (downloaded
     * on first use) and returns a client wired to it. No API token or Docker is needed,
     * which makes this a good fit for local development and CI. By default the sidecar
     * starts a bundled Postgres; pass `databaseUrl` to point it at your own instead.
     *
     * See the [embedded mode guide](https://docs.hatchet.run/v1/embedded).
     * @param embeddedOpts - Options for the embedded engine (version, ports, database, ...).
     * @param config - Optional configuration overrides for the client.
     * @param options - Optional client options.
     * @param axiosConfig - Optional Axios configuration for HTTP requests.
     * @returns A new Hatchet client instance connected to the embedded engine.
     */
    static init(embeddedOpts, config, options, axiosConfig) {
        return __awaiter(this, void 0, void 0, function* () {
            const sidecar = yield startEmbeddedSidecar(embeddedOpts);
            const client = client_1.HatchetClient.init(Object.assign(Object.assign(Object.assign({ token: sidecar.token, tenant_id: sidecar.tenantId, host_port: sidecar.grpcAddress }, (sidecar.apiUrl ? { api_url: sidecar.apiUrl } : {})), { tls_config: { tls_strategy: 'none' } }), config), options, axiosConfig);
            client.stopEmbedded = () => sidecar.stop();
            return client;
        });
    }
}
exports.HatchetEmbeddedClient = HatchetEmbeddedClient;
