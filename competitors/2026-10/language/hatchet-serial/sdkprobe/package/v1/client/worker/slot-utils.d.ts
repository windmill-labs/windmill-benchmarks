import { BaseWorkflowDeclaration } from '../../declaration';
import type { LegacyWorkflow } from '../../../legacy/legacy-transformer';
import { SlotConfig } from '../../slot-types';
export interface WorkerSlotOptions {
    /** (optional) Maximum number of concurrent runs on this worker, defaults to 100 */
    slots?: number;
    /** (optional) Maximum number of concurrent durable tasks, defaults to 1,000 */
    durableSlots?: number;
    /** (optional) Array of workflows to register (supports both v1 and legacy workflow formats) */
    workflows?: Array<BaseWorkflowDeclaration<any, any> | LegacyWorkflow>;
    /** @deprecated Use slots instead */
    maxRuns?: number;
}
export declare function resolveWorkerOptions<T extends WorkerSlotOptions>(options: T): T & {
    slots?: number;
    durableSlots?: number;
    slotConfig: SlotConfig;
};
export declare const testingExports: {
    resolveWorkerOptions: typeof resolveWorkerOptions;
};
