import { BaseWorkflowDeclaration, InputType, OutputType } from './..';
import type { SdkMcpToolDefinition } from '@anthropic-ai/claude-agent-sdk';
import type { CallToolResult, ToolAnnotations } from '@modelcontextprotocol/sdk/types.js';
export type ClaudeToolFuncT = <I extends InputType, O extends OutputType>(runnable: BaseWorkflowDeclaration<I, O>, annotations?: ToolAnnotations) => SdkMcpToolDefinition;
export declare const ClaudeToolFunc: <I extends InputType, O extends OutputType>(runnable: BaseWorkflowDeclaration<I, O>, annotations?: ToolAnnotations) => {
    annotations: {
        title?: string | undefined;
        readOnlyHint?: boolean | undefined;
        destructiveHint?: boolean | undefined;
        idempotentHint?: boolean | undefined;
        openWorldHint?: boolean | undefined;
    } | undefined;
    description: string;
    handler: (args: any, _: unknown) => Promise<CallToolResult>;
    name: string;
    inputSchema: any;
};
