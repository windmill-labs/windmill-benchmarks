/**
 * Pre-eviction fallback for DurableContext.
 *
 * Supports engines older than MinEngineVersion.DURABLE_EVICTION.
 * Remove this module when support for those engines is dropped.
 */
import { Conditions } from '../../../conditions';
import type { DurableListenerClient } from '../../../../clients/listeners/durable-listener/durable-listener-client';
/** The unary and streaming RPCs the pre-eviction fallback needs from the listener. */
export type LegacyDurableTransport = Pick<DurableListenerClient, 'registerDurableEvent' | 'result'>;
export declare function isLegacyDurableTransport(value: unknown): value is LegacyDurableTransport;
export declare function waitForPreEviction(durableListener: LegacyDurableTransport, taskRunExternalId: string, waitKey: number, conditions: Conditions | Conditions[], namespace?: string, signal?: AbortSignal): Promise<{
    result: Record<string, unknown>;
    nextWaitKey: number;
}>;
