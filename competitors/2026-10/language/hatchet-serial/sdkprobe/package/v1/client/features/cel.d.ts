import { V1CELDebugResponseStatus } from '../../../clients/rest/generated/data-contracts';
import { HatchetClient } from '../client';
export type DebugCELInput = {
    expression: string;
    input: Record<string, any>;
    additionalMetadata?: Record<string, string>;
    filterPayload?: Record<string, any>;
};
export type CELEvaluationResult = {
    status: V1CELDebugResponseStatus.SUCCESS;
    output: boolean;
} | {
    status: V1CELDebugResponseStatus.ERROR;
    error: string;
};
/**
 * Client for debugging CEL expressions.
 */
export declare class CELClient {
    api: HatchetClient['api'];
    tenantId: string;
    constructor(client: HatchetClient);
    /**
     * Debug a CEL expression with the provided input, filter payload, and optional metadata. Useful for testing and validating CEL expressions and debugging issues in production.
     *
     * @param input - The input data to evaluate the CEL expression against.
     * @returns A promise that resolves to the evaluation result, which can either be a success with an output or an error with a message.
     * @throws Will throw an error if the input is invalid or the API call fails.
     */
    debug(input: DebugCELInput): Promise<CELEvaluationResult>;
}
