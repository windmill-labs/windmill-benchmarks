import * as z from 'zod/v4';
import { CreateStep } from './step';
import { ConcurrencyLimitStrategy as PbConcurrencyLimitStrategy, StickyStrategy as PbStickyStrategy } from '../protoc/v1/workflows';
declare const StepsSchema: z.ZodArray<z.ZodObject<{
    name: z.ZodString;
    parents: z.ZodOptional<z.ZodArray<z.ZodString>>;
    timeout: z.ZodOptional<z.ZodString>;
    retries: z.ZodOptional<z.ZodNumber>;
    rate_limits: z.ZodOptional<z.ZodArray<z.ZodObject<{
        key: z.ZodOptional<z.ZodString>;
        staticKey: z.ZodOptional<z.ZodString>;
        dynamicKey: z.ZodOptional<z.ZodString>;
        units: z.ZodUnion<readonly [z.ZodNumber, z.ZodString]>;
        limit: z.ZodOptional<z.ZodUnion<readonly [z.ZodNumber, z.ZodString]>>;
        duration: z.ZodOptional<z.ZodEnum<typeof import("..").RateLimitDuration>>;
    }, z.core.$strip>>>;
    worker_labels: z.ZodOptional<z.ZodRecord<z.ZodAny, z.ZodLazy<z.ZodOptional<z.ZodUnion<readonly [z.ZodString, z.ZodNumber, z.ZodObject<{
        value: z.ZodUnion<readonly [z.ZodString, z.ZodNumber]>;
        required: z.ZodOptional<z.ZodBoolean>;
        weight: z.ZodOptional<z.ZodNumber>;
        comparator: z.ZodOptional<z.ZodEnum<typeof import("..").WorkerLabelComparator>>;
    }, z.core.$strip>]>>>>>;
    backoff: z.ZodOptional<z.ZodObject<{
        factor: z.ZodOptional<z.ZodNumber>;
        maxSeconds: z.ZodOptional<z.ZodNumber>;
    }, z.core.$strip>>;
}, z.core.$strip>>;
export type Steps = z.infer<typeof StepsSchema>;
export declare const ConcurrencyLimitStrategy: typeof PbConcurrencyLimitStrategy;
export declare const WorkflowConcurrency: z.ZodObject<{
    name: z.ZodString;
    maxRuns: z.ZodOptional<z.ZodNumber>;
    limitStrategy: z.ZodOptional<z.ZodEnum<typeof PbConcurrencyLimitStrategy>>;
    expression: z.ZodOptional<z.ZodString>;
}, z.core.$strip>;
export declare const HatchetTimeoutSchema: z.ZodString;
export declare const StickyStrategy: typeof PbStickyStrategy;
export declare const CreateWorkflowSchema: z.ZodObject<{
    id: z.ZodString;
    description: z.ZodString;
    version: z.ZodOptional<z.ZodString>;
    sticky: z.ZodOptional<z.ZodUnion<readonly [z.ZodEnum<typeof PbStickyStrategy>, z.ZodEnum<{
        soft: "soft";
        hard: "hard";
    }>]>>;
    scheduleTimeout: z.ZodOptional<z.ZodString>;
    timeout: z.ZodOptional<z.ZodString>;
    on: z.ZodOptional<z.ZodUnion<readonly [z.ZodObject<{
        cron: z.ZodString;
        event: z.ZodOptional<z.ZodUndefined>;
    }, z.core.$strip>, z.ZodObject<{
        cron: z.ZodOptional<z.ZodUndefined>;
        event: z.ZodString;
    }, z.core.$strip>]>>;
    steps: z.ZodArray<z.ZodObject<{
        name: z.ZodString;
        parents: z.ZodOptional<z.ZodArray<z.ZodString>>;
        timeout: z.ZodOptional<z.ZodString>;
        retries: z.ZodOptional<z.ZodNumber>;
        rate_limits: z.ZodOptional<z.ZodArray<z.ZodObject<{
            key: z.ZodOptional<z.ZodString>;
            staticKey: z.ZodOptional<z.ZodString>;
            dynamicKey: z.ZodOptional<z.ZodString>;
            units: z.ZodUnion<readonly [z.ZodNumber, z.ZodString]>;
            limit: z.ZodOptional<z.ZodUnion<readonly [z.ZodNumber, z.ZodString]>>;
            duration: z.ZodOptional<z.ZodEnum<typeof import("..").RateLimitDuration>>;
        }, z.core.$strip>>>;
        worker_labels: z.ZodOptional<z.ZodRecord<z.ZodAny, z.ZodLazy<z.ZodOptional<z.ZodUnion<readonly [z.ZodString, z.ZodNumber, z.ZodObject<{
            value: z.ZodUnion<readonly [z.ZodString, z.ZodNumber]>;
            required: z.ZodOptional<z.ZodBoolean>;
            weight: z.ZodOptional<z.ZodNumber>;
            comparator: z.ZodOptional<z.ZodEnum<typeof import("..").WorkerLabelComparator>>;
        }, z.core.$strip>]>>>>>;
        backoff: z.ZodOptional<z.ZodObject<{
            factor: z.ZodOptional<z.ZodNumber>;
            maxSeconds: z.ZodOptional<z.ZodNumber>;
        }, z.core.$strip>>;
    }, z.core.$strip>>;
    onFailure: z.ZodOptional<z.ZodObject<{
        name: z.ZodString;
        parents: z.ZodOptional<z.ZodArray<z.ZodString>>;
        timeout: z.ZodOptional<z.ZodString>;
        retries: z.ZodOptional<z.ZodNumber>;
        rate_limits: z.ZodOptional<z.ZodArray<z.ZodObject<{
            key: z.ZodOptional<z.ZodString>;
            staticKey: z.ZodOptional<z.ZodString>;
            dynamicKey: z.ZodOptional<z.ZodString>;
            units: z.ZodUnion<readonly [z.ZodNumber, z.ZodString]>;
            limit: z.ZodOptional<z.ZodUnion<readonly [z.ZodNumber, z.ZodString]>>;
            duration: z.ZodOptional<z.ZodEnum<typeof import("..").RateLimitDuration>>;
        }, z.core.$strip>>>;
        worker_labels: z.ZodOptional<z.ZodRecord<z.ZodAny, z.ZodLazy<z.ZodOptional<z.ZodUnion<readonly [z.ZodString, z.ZodNumber, z.ZodObject<{
            value: z.ZodUnion<readonly [z.ZodString, z.ZodNumber]>;
            required: z.ZodOptional<z.ZodBoolean>;
            weight: z.ZodOptional<z.ZodNumber>;
            comparator: z.ZodOptional<z.ZodEnum<typeof import("..").WorkerLabelComparator>>;
        }, z.core.$strip>]>>>>>;
        backoff: z.ZodOptional<z.ZodObject<{
            factor: z.ZodOptional<z.ZodNumber>;
            maxSeconds: z.ZodOptional<z.ZodNumber>;
        }, z.core.$strip>>;
    }, z.core.$strip>>;
}, z.core.$strip>;
/**
 * @deprecated Use client.workflow instead (TODO link to migration doc)
 */
export interface Workflow extends z.infer<typeof CreateWorkflowSchema> {
    concurrency?: z.infer<typeof WorkflowConcurrency> & {
        key?: (ctx: any) => string;
    };
    steps: CreateStep<any, any>[];
    onFailure?: CreateStep<any, any>;
}
export {};
