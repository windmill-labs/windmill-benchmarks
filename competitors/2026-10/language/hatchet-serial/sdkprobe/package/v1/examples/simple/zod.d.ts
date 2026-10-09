import * as z from 'zod/v4';
declare const SimpleInputSchema: z.ZodObject<{
    Message: z.ZodString;
}, z.core.$strip>;
export type SimpleInputWithZod = z.infer<typeof SimpleInputSchema>;
export declare const simpleWithZod: import("../..").TaskWorkflowDeclaration<{
    Message: string;
}, {
    TransformedMessage: string;
}, {}, {}, {}, {}>;
export {};
