import { GetRunDetailsResponse, runStatusToJSON } from '../protoc/v1/workflows';
import type { RunDetail, TaskRunDetail } from './types';
/**
 * A task's output the way the Node client's `result()` reads it (`JSON.parse(output || '{}')`):
 * nothing stored is `{}`, a stored `null` is `null`, and stored text that is not JSON throws
 * the `SyntaxError`, so a corrupt output is never mistaken for an empty one.
 */
export declare function parseTaskOutput(task: TaskRunDetail): unknown;
/**
 * Converts a `GetRunDetails` response into the `RunDetail` both clients return: statuses
 * become `V1TaskStatus` values and the JSON payloads are decoded.
 */
export declare function toRunDetail(raw: GetRunDetailsResponse): RunDetail;
export { runStatusToJSON };
