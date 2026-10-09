/**
 * Shared OTel instrumentor setup.
 *
 * Import and call initOtel() once at process startup (in both worker and run)
 * before using the tracer.
 */
import type { Tracer } from '@opentelemetry/api';
export declare function initOtel(): void;
export declare function getTracer(name: string): Tracer;
