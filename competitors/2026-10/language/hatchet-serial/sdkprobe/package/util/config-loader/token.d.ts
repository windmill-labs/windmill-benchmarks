export declare function getTenantIdFromJWT(token: string): string;
/**
 * The engine address a token was issued with, or `undefined` when the claim is absent. The
 * core client needs only this claim, so it must not fail on a token that lacks `server_url`.
 */
export declare function getGrpcBroadcastAddressFromJWT(token: string): string | undefined;
export declare function getAddressesFromJWT(token: string): {
    serverUrl: string;
    grpcBroadcastAddress: string;
};
