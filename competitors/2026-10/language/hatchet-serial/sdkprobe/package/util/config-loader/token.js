"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getTenantIdFromJWT = getTenantIdFromJWT;
exports.getGrpcBroadcastAddressFromJWT = getGrpcBroadcastAddressFromJWT;
exports.getAddressesFromJWT = getAddressesFromJWT;
const parse_1 = require("../parse");
function getTenantIdFromJWT(token) {
    const claims = extractClaimsFromJWT(token);
    if (claims.sub === undefined) {
        throw new Error('Invalid token: missing sub');
    }
    return claims.sub;
}
/**
 * The engine address a token was issued with, or `undefined` when the claim is absent. The
 * core client needs only this claim, so it must not fail on a token that lacks `server_url`.
 */
function getGrpcBroadcastAddressFromJWT(token) {
    return extractClaimsFromJWT(token).grpc_broadcast_address || undefined;
}
function getAddressesFromJWT(token) {
    const claims = extractClaimsFromJWT(token);
    if (claims.server_url === undefined || claims.grpc_broadcast_address === undefined) {
        throw new Error('Invalid token: missing server_url or grpc_broadcast_address');
    }
    return {
        serverUrl: claims.server_url,
        grpcBroadcastAddress: claims.grpc_broadcast_address,
    };
}
function extractClaimsFromJWT(token) {
    const parts = token.split('.');
    if (parts.length !== 3) {
        throw new Error('Invalid token format');
    }
    const [_, claimsPart] = parts;
    const claimsData = atob(claimsPart.replace(/-/g, '+').replace(/_/g, '/'));
    const claims = (0, parse_1.parseJSON)(claimsData);
    return claims;
}
