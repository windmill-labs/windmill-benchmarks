import type { GenFile, GenMessage } from "@bufbuild/protobuf/codegenv2";
import type { Any } from "@bufbuild/protobuf/wkt";
import type { Message } from "@bufbuild/protobuf";
/**
 * Describes the file google/rpc/status.proto.
 */
export declare const file_google_rpc_status: GenFile;
/**
 * The `Status` type defines a logical error model that is suitable for
 * different programming environments, including REST APIs and RPC APIs. It is
 * used by [gRPC](https://github.com/grpc). Each `Status` message contains
 * three pieces of data: error code, error message, and error details.
 *
 * You can find out more about this error model and how to work with it in the
 * [API Design Guide](https://cloud.google.com/apis/design/errors).
 *
 * @generated from message google.rpc.Status
 */
export type Status = Message<"google.rpc.Status"> & {
    /**
     * The status code, which should be an enum value of
     * [google.rpc.Code][google.rpc.Code].
     *
     * @generated from field: int32 code = 1;
     */
    code: number;
    /**
     * A developer-facing error message, which should be in English. Any
     * user-facing error message should be localized and sent in the
     * [google.rpc.Status.details][google.rpc.Status.details] field, or localized
     * by the client.
     *
     * @generated from field: string message = 2;
     */
    message: string;
    /**
     * A list of messages that carry the error details.  There is a common set of
     * message types for APIs to use.
     *
     * @generated from field: repeated google.protobuf.Any details = 3;
     */
    details: Any[];
};
/**
 * Describes the message google.rpc.Status.
 * Use `create(StatusSchema)` to create a new message.
 */
export declare const StatusSchema: GenMessage<Status>;
