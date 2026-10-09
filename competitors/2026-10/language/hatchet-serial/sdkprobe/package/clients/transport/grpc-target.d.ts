/**
 * Where a gRPC target points: the host and port to dial and the `:authority` grpc-js sends for
 * it, which is the target's path as written (the first address for the IP schemes).
 */
export interface GrpcTarget {
    host: string;
    port: number;
    authority: string;
}
/**
 * Parses the target forms grpc-js accepts for `host_port`, so the unary transport dials the
 * same endpoint the streaming channel does: `host:port`, `dns:host:port`, `dns:///host:port`,
 * `ipv4:address:port` and `ipv6:[address]:port` (the first address of an address list). A
 * missing port is grpc-js's default, 443. `unix:` sockets and a `dns://nameserver/` authority
 * have no HTTP/2 equivalent here and are refused with a clear error.
 */
export declare function parseGrpcTarget(target: string): GrpcTarget;
/** The base URL Connect dials for a target: an IPv6 host is bracketed as a URL requires. */
export declare function grpcTargetBaseUrl(target: GrpcTarget, protocol: 'http' | 'https'): string;
