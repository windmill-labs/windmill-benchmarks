"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.createActionId = createActionId;
exports.workflowNameFromAction = workflowNameFromAction;
exports.createAction = createAction;
/**
 * Builds the canonical action id for a task: `<workflow name>:<task name>`, fully
 * lowercased. The engine lowercases action ids on registration and the Go SDK sends
 * them lowercased, so this is the one form used for registration, the worker's
 * action registries and the ids the dispatcher assigns.
 */
function createActionId(workflowName, taskName) {
    return `${workflowName}:${taskName}`.toLowerCase();
}
function workflowNameFromAction(action) {
    const separatorIndex = action.actionId.lastIndexOf(':');
    return separatorIndex === -1 ? action.jobName : action.actionId.substring(0, separatorIndex);
}
function createAction(assignedAction) {
    const action = assignedAction;
    Object.defineProperty(action, 'key', {
        get() {
            // Durable task invocations each get a distinct key so a restored
            // invocation never collides with the one it replaces in contexts,
            // futures, or eviction state.
            if (this.durableTaskInvocationCount !== undefined) {
                return `${this.taskRunExternalId}/${this.retryCount}/${this.durableTaskInvocationCount}`;
            }
            return `${this.taskRunExternalId}/${this.retryCount}`;
        },
        enumerable: true,
        configurable: true,
    });
    return action;
}
