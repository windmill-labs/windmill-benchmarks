"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.testingExports = void 0;
exports.resolveWorkerOptions = resolveWorkerOptions;
const declaration_1 = require("../../declaration");
const slot_types_1 = require("../../slot-types");
const DEFAULT_DEFAULT_SLOTS = 100;
const DEFAULT_DURABLE_SLOTS = 1000;
function resolveWorkerOptions(options) {
    const requiredSlotTypes = options.workflows
        ? getRequiredSlotTypes(options.workflows)
        : new Set();
    const slotConfig = options.slots || options.durableSlots || options.maxRuns
        ? Object.assign(Object.assign({}, (options.slots || options.maxRuns
            ? { [slot_types_1.SlotType.Default]: options.slots || options.maxRuns || 0 }
            : {})), (options.durableSlots ? { [slot_types_1.SlotType.Durable]: options.durableSlots } : {})) : {};
    if (requiredSlotTypes.has(slot_types_1.SlotType.Default) && slotConfig[slot_types_1.SlotType.Default] == null) {
        slotConfig[slot_types_1.SlotType.Default] = DEFAULT_DEFAULT_SLOTS;
    }
    if (requiredSlotTypes.has(slot_types_1.SlotType.Durable) && slotConfig[slot_types_1.SlotType.Durable] == null) {
        slotConfig[slot_types_1.SlotType.Durable] = DEFAULT_DURABLE_SLOTS;
    }
    if (Object.keys(slotConfig).length === 0) {
        slotConfig[slot_types_1.SlotType.Default] = DEFAULT_DEFAULT_SLOTS;
    }
    return Object.assign(Object.assign({}, options), { slots: options.slots ||
            options.maxRuns ||
            (slotConfig[slot_types_1.SlotType.Default] != null ? slotConfig[slot_types_1.SlotType.Default] : undefined), durableSlots: options.durableSlots ||
            (slotConfig[slot_types_1.SlotType.Durable] != null ? slotConfig[slot_types_1.SlotType.Durable] : undefined), slotConfig });
}
exports.testingExports = {
    resolveWorkerOptions,
};
function getRequiredSlotTypes(workflows) {
    const required = new Set();
    const addFromRequests = (requests, fallbackType) => {
        if (requests && Object.keys(requests).length > 0) {
            if (requests[slot_types_1.SlotType.Default] !== undefined) {
                required.add(slot_types_1.SlotType.Default);
            }
            if (requests[slot_types_1.SlotType.Durable] !== undefined) {
                required.add(slot_types_1.SlotType.Durable);
            }
        }
        else {
            required.add(fallbackType);
        }
    };
    for (const wf of workflows) {
        if (wf instanceof declaration_1.BaseWorkflowDeclaration) {
            const tasks = wf.definition['_tasks'];
            for (const task of tasks) {
                addFromRequests(task.slotRequests, slot_types_1.SlotType.Default);
            }
            const durableTasks = wf.definition['_durableTasks'];
            if (durableTasks.length > 0) {
                required.add(slot_types_1.SlotType.Durable);
            }
            if (wf.definition.onFailure) {
                const opts = typeof wf.definition.onFailure === 'object' ? wf.definition.onFailure : undefined;
                addFromRequests(opts === null || opts === void 0 ? void 0 : opts.slotRequests, slot_types_1.SlotType.Default);
            }
            if (wf.definition.onSuccess) {
                const opts = typeof wf.definition.onSuccess === 'object' ? wf.definition.onSuccess : undefined;
                addFromRequests(opts === null || opts === void 0 ? void 0 : opts.slotRequests, slot_types_1.SlotType.Default);
            }
        }
    }
    return required;
}
