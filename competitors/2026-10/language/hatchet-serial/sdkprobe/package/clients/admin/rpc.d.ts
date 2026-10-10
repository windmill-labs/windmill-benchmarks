import { WorkflowServiceClient } from '../../protoc/workflows/workflows';
import { AdminServiceClient } from '../../protoc/v1/workflows';
import type { Transport } from '../transport/transport';
/**
 * The `WorkflowService` client, every RPC of the generated `WorkflowServiceClient` interface,
 * served by a Connect client on the given transport.
 */
export declare function createWorkflowsRpc(transport: Transport): WorkflowServiceClient;
/**
 * The v1 `AdminService` client, every RPC of the generated `AdminServiceClient` interface,
 * served by a Connect client on the given transport.
 */
export declare function createV1AdminRpc(transport: Transport): AdminServiceClient;
