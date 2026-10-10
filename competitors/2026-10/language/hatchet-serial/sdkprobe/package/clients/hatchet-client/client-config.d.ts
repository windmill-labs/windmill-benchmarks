import { ChannelCredentials } from 'nice-grpc';
import { z } from 'zod/v4';
import type { Context } from '../../v1/client/worker/context';
import { Logger, LogLevel } from '../../util/logger';
export declare const RetrierConfigSchema: z.ZodObject<{
    maxAttempts: z.ZodOptional<z.ZodNumber>;
    initialInterval: z.ZodOptional<z.ZodNumber>;
    maxJitter: z.ZodOptional<z.ZodNumber>;
}, z.core.$strip>;
export type RetrierConfig = z.infer<typeof RetrierConfigSchema>;
declare const ClientTLSConfigSchema: z.ZodObject<{
    tls_strategy: z.ZodOptional<z.ZodEnum<{
        tls: "tls";
        mtls: "mtls";
        none: "none";
    }>>;
    cert_file: z.ZodOptional<z.ZodString>;
    ca_file: z.ZodOptional<z.ZodString>;
    key_file: z.ZodOptional<z.ZodString>;
    server_name: z.ZodOptional<z.ZodString>;
}, z.core.$strip>;
export declare const OpenTelemetryConfigSchema: z.ZodObject<{
    excludedAttributes: z.ZodDefault<z.ZodOptional<z.ZodArray<z.ZodString>>>;
    includeTaskNameInSpanName: z.ZodDefault<z.ZodOptional<z.ZodBoolean>>;
    individualRunSpansForBulkRun: z.ZodDefault<z.ZodOptional<z.ZodBoolean>>;
}, z.core.$strip>;
export type OpenTelemetryConfig = z.infer<typeof OpenTelemetryConfigSchema>;
export declare const ClientConfigSchema: z.ZodObject<{
    token: z.ZodString;
    tls_config: z.ZodObject<{
        tls_strategy: z.ZodOptional<z.ZodEnum<{
            tls: "tls";
            mtls: "mtls";
            none: "none";
        }>>;
        cert_file: z.ZodOptional<z.ZodString>;
        ca_file: z.ZodOptional<z.ZodString>;
        key_file: z.ZodOptional<z.ZodString>;
        server_name: z.ZodOptional<z.ZodString>;
    }, z.core.$strip>;
    healthcheck: z.ZodOptional<z.ZodObject<{
        enabled: z.ZodDefault<z.ZodOptional<z.ZodBoolean>>;
        port: z.ZodDefault<z.ZodOptional<z.ZodNumber>>;
    }, z.core.$strip>>;
    host_port: z.ZodString;
    api_url: z.ZodString;
    log_level: z.ZodOptional<z.ZodEnum<{
        OFF: "OFF";
        DEBUG: "DEBUG";
        INFO: "INFO";
        WARN: "WARN";
        ERROR: "ERROR";
    }>>;
    tenant_id: z.ZodString;
    namespace: z.ZodOptional<z.ZodString>;
    otel: z.ZodOptional<z.ZodObject<{
        excludedAttributes: z.ZodDefault<z.ZodOptional<z.ZodArray<z.ZodString>>>;
        includeTaskNameInSpanName: z.ZodDefault<z.ZodOptional<z.ZodBoolean>>;
        individualRunSpansForBulkRun: z.ZodDefault<z.ZodOptional<z.ZodBoolean>>;
    }, z.core.$strip>>;
    middleware: z.ZodOptional<z.ZodObject<{
        before: z.ZodOptional<z.ZodAny>;
        after: z.ZodOptional<z.ZodAny>;
    }, z.core.$strip>>;
    cancellation_grace_period: z.ZodDefault<z.ZodOptional<z.ZodNumber>>;
    cancellation_warning_threshold: z.ZodDefault<z.ZodOptional<z.ZodNumber>>;
    grpc_max_recv_message_length: z.ZodDefault<z.ZodOptional<z.ZodNumber>>;
    grpc_max_send_message_length: z.ZodDefault<z.ZodOptional<z.ZodNumber>>;
    retrier: z.ZodOptional<z.ZodObject<{
        maxAttempts: z.ZodOptional<z.ZodNumber>;
        initialInterval: z.ZodOptional<z.ZodNumber>;
        maxJitter: z.ZodOptional<z.ZodNumber>;
    }, z.core.$strip>>;
}, z.core.$strip>;
export type LogConstructor = (context: string, logLevel?: LogLevel) => Logger;
/**
 * A middleware function that runs before every task invocation.
 * Returns extra fields to replace the task input, or void to skip.
 * @template T - The expected input type for the hook.
 * @param input - The current task input.
 * @param ctx - The task execution context.
 * @returns The new input value, or void to pass through unchanged.
 */
export type BeforeHookFn<T = any> = (input: T, ctx: Context<any>) => Record<string, any> | void | Promise<Record<string, any> | void>;
/**
 * A middleware function that runs after every task invocation.
 * Returns extra fields to replace the task output, or void to skip.
 * @param output - The task output.
 * @param ctx - The task execution context.
 * @param input - The task input (after before-hooks have run).
 * @returns The new output value, or void to pass through unchanged.
 */
export type AfterHookFn<TOutput = any, TInput = any> = (output: TOutput, ctx: Context<any>, input: TInput) => Record<string, any> | void | Promise<Record<string, any> | void>;
/**
 * Middleware hooks that run before/after every task invocation.
 *
 * Each hook can be a single function or an array of functions.
 * When an array is provided the functions run in order and each
 * result replaces the value (input for `before`, output for `after`).
 *
 * Return `void` (or `undefined`) from a hook to pass through unchanged.
 */
export type TaskMiddleware<TInput = any, TOutput = any> = {
    before?: BeforeHookFn<TInput> | readonly BeforeHookFn<TInput>[];
    after?: AfterHookFn<TOutput, TInput> | readonly AfterHookFn<TOutput, TInput>[];
};
type NonVoidReturn<F> = F extends (...args: any[]) => infer R ? Exclude<Awaited<R>, void | undefined> : {};
type MergeReturns<T> = T extends readonly [infer F, ...infer Rest] ? NonVoidReturn<F> & MergeReturns<Rest> : {};
export type InferMiddlewareBefore<M> = M extends {
    before: infer P;
} ? P extends (...args: any[]) => any ? NonVoidReturn<P> : P extends readonly any[] ? MergeReturns<P> : {} : {};
export type InferMiddlewareAfter<M> = M extends {
    after: infer P;
} ? P extends (...args: any[]) => any ? NonVoidReturn<P> : P extends readonly any[] ? MergeReturns<P> : {} : {};
type ClientConfigInferred = z.infer<typeof ClientConfigSchema>;
export type ClientConfig = Omit<ClientConfigInferred, 'cancellation_grace_period' | 'cancellation_warning_threshold' | 'grpc_max_recv_message_length' | 'grpc_max_send_message_length'> & {
    cancellation_grace_period?: number;
    cancellation_warning_threshold?: number;
    grpc_max_recv_message_length?: number;
    grpc_max_send_message_length?: number;
} & {
    credentials?: ChannelCredentials;
} & {
    logger: LogConstructor;
    middleware?: TaskMiddleware;
};
export type ClientTLSConfig = z.infer<typeof ClientTLSConfigSchema>;
export {};
