import type { Workflow } from './workflow';
import { BaseWorkflowDeclaration, WorkflowDeclaration } from '../v1/declaration';
export type { Workflow as LegacyWorkflow } from './workflow';
/**
 * Type guard: returns true if the workflow is a legacy v0 Workflow (not a BaseWorkflowDeclaration).
 */
export declare function isLegacyWorkflow(workflow: unknown): workflow is Workflow;
/**
 * Warns once per process that a v0 workflow is in use: the banner for the terminal and a
 * DeprecationWarning with a stable code that Node's warning flags and handlers can filter.
 */
export declare function warnLegacyWorkflow(): void;
/**
 * Transforms a legacy v0 Workflow into a v1 WorkflowDeclaration.
 *
 * The transformed declaration can be registered with a worker and executed
 * by the v1 runtime. Legacy step `run` functions are wrapped to receive
 * a V0Context for backwards compatibility.
 */
export declare function transformLegacyWorkflow(workflow: Workflow): WorkflowDeclaration<any, any>;
/**
 * Normalizes a workflow: if it is a legacy Workflow, emits a deprecation warning
 * and transforms it into a BaseWorkflowDeclaration. If already a v1 declaration,
 * returns it as-is.
 */
export declare function normalizeWorkflow(workflow: BaseWorkflowDeclaration<any, any> | Workflow): BaseWorkflowDeclaration<any, any>;
/**
 * Normalizes an array of workflows, transforming any legacy workflows and
 * emitting deprecation warnings.
 */
export declare function normalizeWorkflows(workflows: Array<BaseWorkflowDeclaration<any, any> | Workflow>): Array<BaseWorkflowDeclaration<any, any>>;
/**
 * Extracts the workflow name from a workflow-like value.
 * Works for strings, BaseWorkflowDeclaration, WorkflowDefinition, and legacy Workflow.
 * Emits a deprecation warning if a legacy workflow is detected.
 */
export declare function getWorkflowName(workflow: string | BaseWorkflowDeclaration<any, any> | Workflow | {
    name: string;
}): string;
