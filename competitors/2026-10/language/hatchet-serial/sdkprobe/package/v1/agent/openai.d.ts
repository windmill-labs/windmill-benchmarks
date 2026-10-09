import { BaseWorkflowDeclaration, InputType, OutputType } from './..';
import type { FunctionTool } from '@openai/agents';
export type OpenAIToolFuncT = <I extends InputType, O extends OutputType>(runnable: BaseWorkflowDeclaration<I, O>) => FunctionTool;
export declare const OpenAIToolFunc: <I extends InputType, O extends OutputType>(runnable: BaseWorkflowDeclaration<I, O>) => any;
