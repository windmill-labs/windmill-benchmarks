import { z } from 'zod/v4';
declare const TemperatureInput: z.ZodObject<{
    locationName: z.ZodString;
    coords: z.ZodObject<{
        latitude: z.ZodNumber;
        longitude: z.ZodNumber;
    }, z.core.$strip>;
}, z.core.$strip>;
export type TemperatureInputWithZod = z.infer<typeof TemperatureInput>;
export declare const getTemperatureWorkflow: import("../..").WorkflowDeclaration<{
    locationName: string;
    coords: {
        latitude: number;
        longitude: number;
    };
}, {}, {}>;
export declare const getTemperature: import("../..").TaskWorkflowDeclaration<{
    locationName: string;
    coords: {
        latitude: number;
        longitude: number;
    };
}, {
    text: string;
}, {}, {}, {}, {}>;
export declare function createTemperatureWorkflowToolClaude(): import("@anthropic-ai/claude-agent-sdk").SdkMcpToolDefinition;
export declare function createTemperatureWorkflowToolOpenai(): import("@openai/agents").FunctionTool;
export declare function createTemperatureTaskToolClaude(): import("@anthropic-ai/claude-agent-sdk").SdkMcpToolDefinition;
export declare function createTemperatureTaskToolOpenai(): import("@openai/agents").FunctionTool;
export {};
