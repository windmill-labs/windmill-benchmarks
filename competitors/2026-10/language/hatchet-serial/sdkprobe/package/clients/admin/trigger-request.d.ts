import { ConnectError } from '@connectrpc/connect';
import { BulkTriggerIdempotencyCollisionError } from '../../util/errors/bulk-trigger-idempotency-collision-error';
import type { TriggerWorkflowRequest } from '../../protoc/v1/shared/trigger';
import type { DeepPartial } from '../transport/message-bridge';
import type { TriggerRunOptions } from '../../core/types';
/**
 * One builder for both clients, so a run triggered from the core client is indistinguishable
 * to the engine from one triggered by the Node client.
 */
export declare function buildTriggerWorkflowRequest<Q>(workflowName: string, input: Q, options: TriggerRunOptions | undefined, namespace: string | undefined): DeepPartial<TriggerWorkflowRequest>;
/**
 * The engine answers a trigger whose idempotency key is already taken with `ALREADY_EXISTS`
 * and attaches the collision details to the status. The Connect client decodes those details
 * from the trailer, so the error carries everything needed to build the SDK's collision errors.
 */
export declare function isAlreadyExists(e: unknown): e is ConnectError;
export declare function extractExistingRunId(e: ConnectError): string;
export declare function extractBulkTriggerCollision(e: ConnectError): BulkTriggerIdempotencyCollisionError | null;
