import { type RetrierConfig } from '../../util/retrier';
import type { Logger } from '../../util/logger/logger';
import type { AdminServiceClient, CreateWorkflowVersionRequest, CreateWorkflowVersionResponse } from '../../protoc/v1/workflows';
import type { BaseWorkflowDeclaration, WorkflowDefinition } from '../../v1/declaration';
import type { InputType, OutputType } from '../../v1/types';
export interface WorkflowsClientDeps {
    rpc: AdminServiceClient;
    logger: Logger;
    namespace?: string;
    retrier?: RetrierConfig;
}
/** Registers workflow definitions with the engine. */
export declare class WorkflowsClient {
    private readonly deps;
    constructor(deps: WorkflowsClientDeps);
    /**
     * Creates or updates a workflow. A declaration from `/edge` (or its definition) is
     * converted with the client's namespace the way a worker registers it; a
     * `CreateWorkflowVersionRequest` is sent as given.
     */
    put<I extends InputType, O extends OutputType>(workflow: CreateWorkflowVersionRequest | WorkflowDefinition | BaseWorkflowDeclaration<I, O>): Promise<CreateWorkflowVersionResponse>;
}
