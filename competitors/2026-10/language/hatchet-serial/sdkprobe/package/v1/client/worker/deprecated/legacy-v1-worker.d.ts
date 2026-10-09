/**
 * Legacy V1Worker subclass that registers with the old `slots` proto field
 * instead of `slotConfig`. Used when connected to pre-slot-config engines.
 */
import { ActionListener } from '../../../../clients/dispatcher/action-listener';
import { HatchetClient } from '../../..';
import { InternalWorker } from '../worker-internal';
export declare class LegacyV1Worker extends InternalWorker {
    private _legacySlotCount;
    constructor(client: HatchetClient, options: ConstructorParameters<typeof InternalWorker>[1], legacySlots: number);
    /**
     * Override registration to use the legacy `slots` proto field.
     */
    protected createListener(): Promise<ActionListener>;
}
