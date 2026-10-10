"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.toProtobufEs = toProtobufEs;
exports.fromProtobufEs = fromProtobufEs;
const protobuf_1 = require("@bufbuild/protobuf");
/**
 * Converts a `ts-proto` message to the protobuf-es message a Connect client sends. The two
 * bindings share the wire format, so an encode/decode round trip is an exact translation and
 * the SDK keeps its `ts-proto` types as the public request types.
 */
function toProtobufEs(schema, codec, message) {
    return (0, protobuf_1.fromBinary)(schema, codec.encode(codec.fromPartial(message)).finish());
}
/**
 * Converts a protobuf-es message a Connect client received to the `ts-proto` message callers
 * expect, the inverse of `toProtobufEs`.
 */
function fromProtobufEs(codec, schema, message) {
    return codec.decode((0, protobuf_1.toBinary)(schema, message));
}
