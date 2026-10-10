/**
 * Runtime-neutral helpers for the actions a worker receives from the dispatcher.
 *
 * This module imports nothing from Node so it can be used from the edge entry point.
 */
import type { AssignedAction } from '../../protoc/dispatcher';
export type ActionKey = `${string}/${number}` | `${string}/${number}/${number}`;
export type Action = AssignedAction & {
    readonly key: ActionKey;
};
/**
 * Builds the canonical action id for a task: `<workflow name>:<task name>`, fully
 * lowercased. The engine lowercases action ids on registration and the Go SDK sends
 * them lowercased, so this is the one form used for registration, the worker's
 * action registries and the ids the dispatcher assigns.
 */
export declare function createActionId(workflowName: string, taskName: string): string;
export declare function workflowNameFromAction(action: Pick<AssignedAction, 'actionId' | 'jobName'>): string;
export declare function createAction(assignedAction: AssignedAction): Action;
