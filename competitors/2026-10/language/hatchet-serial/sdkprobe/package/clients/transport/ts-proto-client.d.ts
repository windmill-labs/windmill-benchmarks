import type { DescService } from '@bufbuild/protobuf';
import { type CallOptions as ConnectCallOptions, type Transport } from '@connectrpc/connect';
import type { Client, TsProtoServiceDefinition } from 'nice-grpc';
import { Metadata, type CallOptions } from 'nice-grpc-common';
/**
 * The per-call options the Connect-backed clients accept: `nice-grpc`'s `CallOptions`
 * (`signal`, `metadata`, `onHeader`, `onTrailer`) plus the `deadline` its deadline middleware
 * adds, an absolute time as a `Date` or epoch milliseconds.
 */
export interface UnaryCallOptionsExt {
    deadline?: Date | number;
}
export type UnaryCallOptions = CallOptions & UnaryCallOptionsExt;
/**
 * Builds the client a `ts-proto` service definition describes, with every method served by a
 * Connect client for the matching protobuf-es service on `transport`. The result has the exact
 * surface `nice-grpc`'s `ClientFactory.create(definition, channel)` produced: every RPC of the
 * service as `(request, options?) => Promise<response>` with the SDK's `ts-proto` message types,
 * so a caller holding one of the public client properties sees no difference.
 *
 * Both bindings are generated from the same proto, so a method's client name is the same in
 * each and the protobuf-es descriptor can be looked up by it.
 */
export declare function createTsProtoClient<Def extends TsProtoServiceDefinition>(definition: Def, service: DescService, transport: Transport): Client<Def, UnaryCallOptionsExt>;
/**
 * Translates `nice-grpc` call options to Connect's: the signal passes through, an absolute
 * deadline becomes the remaining time, request metadata becomes headers and the header and
 * trailer callbacks receive `Metadata` built from the response headers.
 */
export declare function toConnectCallOptions(options?: UnaryCallOptions): ConnectCallOptions | undefined;
/**
 * gRPC metadata as HTTP headers: binary values (keys ending in `-bin`) are base64 encoded,
 * which is how they travel on the wire in every gRPC implementation.
 */
export declare function metadataToHeaders(metadata: Metadata): Headers;
/**
 * Response headers as gRPC metadata, the inverse of `metadataToHeaders`. `Headers` joins
 * repeated values with a comma, so a binary header is split back into its values before each
 * is decoded.
 */
export declare function headersToMetadata(headers: Headers): Metadata;
