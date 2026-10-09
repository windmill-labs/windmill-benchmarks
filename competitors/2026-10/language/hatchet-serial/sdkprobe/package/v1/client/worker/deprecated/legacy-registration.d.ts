/**
 * Legacy worker registration using the deprecated `slots` proto field
 * instead of `slotConfig`. For backward compatibility with engines
 * that do not support multiple slot types.
 */
import { DispatcherClient, WorkerLabels } from '../../../../clients/dispatcher/dispatcher-client';
import { ActionListener } from '../../../../clients/dispatcher/action-listener';
export interface LegacyRegistrationOptions {
    workerName: string;
    services: string[];
    actions: string[];
    slots: number;
    labels: WorkerLabels;
}
/**
 * Registers a worker using the legacy `slots` proto field instead of `slotConfig`.
 */
export declare function legacyGetActionListener(dispatcher: DispatcherClient, options: LegacyRegistrationOptions): Promise<ActionListener>;
