import type { AxiosRequestConfig } from 'axios';
import type { ClientConfig, HatchetClientOptions } from '../clients/hatchet-client';
import { HatchetClient } from './client/client';
/**
 * Options for the embedded engine sidecar. All fields are optional; by default the
 * latest hatchet-embedded release is downloaded and started with a bundled Postgres.
 */
export interface EmbeddedOptions {
    /**
     * hatchet-embedded release tag to download (defaults to HATCHET_CLIENT_EMBEDDED_VERSION or
     * latest). Tags correspond to the Hatchet engine version baked into the sidecar,
     * so pinning this pins the engine.
     */
    version?: string;
    /** path to an existing sidecar binary, skips the download (or HATCHET_CLIENT_EMBEDDED_BINARY_PATH) */
    binaryPath?: string;
    /**
     * expected sha256 hex digest of the sidecar binary. When set, it replaces the
     * release's checksums.txt as the trust anchor, so a compromised release
     * channel cannot substitute the binary. Pin it together with `version`.
     */
    checksum?: string;
    /** use an existing Postgres instead of the bundled one */
    databaseUrl?: string;
    /** store the bundled Postgres runtime and data under this directory */
    postgresDataDir?: string;
    /** bind the engine's gRPC server to this port */
    grpcPort?: number;
    /** bind the REST API server to this port */
    apiPort?: number;
    /** set to false to start only the engine + gRPC, no REST API */
    startApi?: boolean;
    /** set to false to skip running migrations on startup */
    runMigrations?: boolean;
    /** use RabbitMQ instead of the Postgres message queue */
    rabbitmqUrl?: string;
    /** log level for the engine's output */
    logLevel?: string;
    /** how long to wait for the engine to become ready, in milliseconds (default 300000) */
    readyTimeoutMs?: number;
}
/**
 * A running embedded engine sidecar and its connection details, as returned by
 * `startEmbeddedSidecar`.
 */
export interface EmbeddedSidecar {
    /** API token for the sidecar's default tenant */
    token: string;
    /** ID of the sidecar's default tenant */
    tenantId: string;
    /** host:port of the engine's gRPC server */
    grpcAddress: string;
    /** base URL of the REST API (empty when `startApi` is false) */
    apiUrl: string;
    /** gracefully stops the sidecar and resolves once it has fully exited */
    stop: () => Promise<void>;
}
/**
 * Gracefully stops every sidecar started in this process by
 * `HatchetEmbeddedClient.init()` (or `startEmbeddedSidecar`) and resolves once
 * they have fully exited, including their bundled Postgres. Call this before
 * your process exits so the engine's shutdown output does not print after
 * your program has returned.
 */
export declare function stopEmbeddedSidecar(): Promise<void>;
/**
 * Downloads (and caches) the hatchet-embedded sidecar binary, spawns it, and waits
 * until the embedded engine is ready. The sidecar shuts down when this process exits.
 * Use {@link HatchetEmbeddedClient.init} unless you need the raw connection details.
 */
export declare function startEmbeddedSidecar(opts?: EmbeddedOptions): Promise<EmbeddedSidecar>;
/**
 * Entry point for embedded mode. `init()` starts a full local Hatchet engine and
 * returns a regular Hatchet client connected to it. No API token or Docker is needed.
 * See the [embedded mode guide](https://docs.hatchet.run/v1/embedded).
 */
export declare class HatchetEmbeddedClient {
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
    static init<T extends Record<string, any> = {}, U extends Record<string, any> = {}>(embeddedOpts?: EmbeddedOptions, config?: Omit<Partial<ClientConfig>, 'middleware'>, options?: HatchetClientOptions, axiosConfig?: AxiosRequestConfig): Promise<EmbeddedClient<T, U>>;
}
/**
 * A `HatchetClient` connected to an embedded engine, extended with `stopEmbedded`.
 * Returned by {@link HatchetEmbeddedClient.init}.
 */
export type EmbeddedClient<T extends Record<string, any> = {}, U extends Record<string, any> = {}> = HatchetClient<T, U> & {
    /**
     * Gracefully stops the embedded engine sidecar and resolves once it has
     * fully exited, including its bundled Postgres. Call this before your
     * process exits so the engine's shutdown output does not print after your
     * program has returned.
     */
    stopEmbedded: () => Promise<void>;
};
