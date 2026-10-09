/**
 * Generic time-aware deprecation helper.
 *
 * Timeline (from a given start date, with configurable windows):
 *   0 to warnDays:              WARNING logged once per feature
 *   warnDays to errorDays:      ERROR logged once per feature
 *   after errorDays:            throws an error 1-in-5 calls (20% chance)
 *
 * Defaults: warnDays=90, errorDays=undefined (error phase disabled unless set).
 */
import { Logger } from '../../../../util/logger';
export declare class DeprecationError extends Error {
    feature: string;
    constructor(feature: string, message: string);
}
export interface DeprecationOpts {
    /** Days after start during which a warning is logged. Defaults to 90. */
    warnDays?: number;
    /** Days after start during which an error is logged.
     *  After this window, calls have a 20% chance of throwing.
     *  If undefined (default), the error/raise phase is never reached —
     *  the notice stays at error-level logging indefinitely. */
    errorDays?: number;
}
/**
 * Emit a time-aware deprecation notice.
 *
 * @param feature - A short identifier for deduplication (each feature logs once).
 * @param message - The human-readable deprecation message.
 * @param start   - The Date when the deprecation window began.
 * @param logger  - A Logger instance for outputting warnings/errors.
 * @param opts    - Optional configuration for time windows.
 * @throws DeprecationError after the errorDays window (~20% chance).
 */
/**
 * Parses a semver string like "v0.78.23" into [major, minor, patch].
 * Returns [0, 0, 0] if parsing fails.
 */
export declare function parseSemver(v: string): [number, number, number];
/**
 * Returns true if semver string a is strictly less than b.
 */
export declare function semverLessThan(a: string, b: string): boolean;
export declare function emitDeprecationNotice(feature: string, message: string, start: Date, logger: Logger, opts?: DeprecationOpts): void;
