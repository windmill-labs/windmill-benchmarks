import { type Transport } from './transport';
/**
 * How a fetch-based client reaches the engine. `none` speaks plain HTTP; `tls` speaks HTTPS
 * and verifies the server the way the runtime's `fetch` does. `serverName` is accepted so the
 * config mirrors the Node client's `tls_config`, but `fetch` has no way to verify a
 * certificate against a name other than the URL host, so it is only honored by a transport
 * that can (a custom `transport` passed to the client).
 */
export interface FetchTlsConfig {
    strategy: 'none' | 'tls';
    serverName?: string;
}
export interface FetchTransportOptions {
    /** The tenant API token, sent as a bearer token on every call. */
    token: string;
    /**
     * The engine's base URL, for example `https://engine.example.com:7070`: an absolute
     * `http://` or `https://` URL with no username, password, query string or fragment. The RPC
     * paths are appended to it as given, minus trailing slashes. Its scheme must agree with
     * `tls`: `http://` needs `{ strategy: 'none' }`, since the token travels on every call.
     */
    serverUrl?: string;
    /** The engine's `host:port`; used with `tls` to build the base URL when `serverUrl` is unset. */
    hostPort?: string;
    /**
     * Defaults to `tls`. `hostPort` takes its scheme from it (`https://` unless `none`), and a
     * `serverUrl` must agree with it.
     */
    tls?: FetchTlsConfig;
    /** The `fetch` to send requests with. Defaults to the runtime's global `fetch`. */
    fetch?: typeof globalThis.fetch;
    /**
     * The largest response accepted, in bytes after the runtime's HTTP decompression. Defaults
     * to 4 MiB, the Node transport's `grpc_max_recv_message_length` default.
     */
    maxReceiveMessageBytes?: number;
    /**
     * The largest request sent, in bytes. Defaults to 4 MiB, the Node transport's
     * `grpc_max_send_message_length` default.
     */
    maxSendMessageBytes?: number;
}
/** The limits `limitMessageSizes` applies, in bytes. */
export interface MessageSizeLimits {
    receive: number;
    send: number;
}
/**
 * Resolves the engine's base URL from a `serverUrl` or from `hostPort` plus the TLS
 * strategy, the two ways a fetch-based client can be pointed at an engine. `hostPort` takes
 * the gRPC target forms the Node client takes (`host:port`, `dns:///host:port`, `ipv4:`,
 * `ipv6:`), so a token or config shared with a Node client works unchanged. A `serverUrl` is
 * checked against the rules its documentation states, so a plaintext URL cannot slip past
 * the TLS default.
 */
export declare function resolveServerUrl(options: Pick<FetchTransportOptions, 'serverUrl' | 'hostPort' | 'tls'>): string;
/**
 * The transport for runtimes that only have `fetch` (Cloudflare Workers, Vercel Functions,
 * Deno, Bun, browsers). It must not depend on the HTTP version, since the engine answers unary
 * calls over whichever of HTTP/1.1 and HTTP/2 the runtime negotiates, and it enforces the Node
 * transport's message size limits itself because connect-web has none.
 */
export declare function createFetchTransport(options: FetchTransportOptions): Transport;
/**
 * Wraps a `fetch` with the message size limits the Node transport enforces through Connect's
 * `readMaxBytes` and `writeMaxBytes`, which the fetch transport has no option for. A request
 * body over the send limit is refused before anything is sent, and a response body that grows
 * past the receive limit is cancelled as it streams, counted after the runtime's HTTP
 * decompression so a small compressed body cannot expand past it; the limit covers Connect
 * error bodies as well as messages. Both surface as a `ConnectError` with
 * `Code.ResourceExhausted`, as they do on the Node transport.
 */
export declare function limitMessageSizes(send: typeof globalThis.fetch, limits: MessageSizeLimits): typeof globalThis.fetch;
