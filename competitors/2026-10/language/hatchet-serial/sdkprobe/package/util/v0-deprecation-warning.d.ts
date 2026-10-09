/**
 * The v0 SDK deprecation warning, emitted when a v0 workflow is actually used (registered on a
 * worker or put to the engine), never when a module is merely imported: the root entry still
 * re-exports the v0 `workflow` and `step` modules for consumers that only use v1 APIs, and
 * loading them must stay silent.
 *
 * `process.emitWarning` with a fixed code keeps the warning filterable with Node's standard
 * `--no-deprecation` and `--no-warnings` flags and addressable from `process.on('warning')`.
 */
export declare const V0_DEPRECATION_CODE = "HATCHET_V0_REMOVED";
/** Reset hook for tests. Not part of the public API. */
export declare function _resetEmittedV0Warnings(): void;
/**
 * Emit a deduplicated v0-removal deprecation warning for a given submodule.
 *
 * Each unique `submodule` is emitted at most once per process. Uses
 * `process.emitWarning` when available so consumers can suppress or filter
 * via standard Node mechanisms.
 *
 * Importing the SDK must never abort module evaluation, since the root
 * specifier still re-exports v0 `workflow` and `step` for transitive
 * consumers who only use v1 APIs. Two cases would otherwise crash them:
 *
 *   1. `process.throwDeprecation` (set by `--throw-deprecation` or directly).
 *      Node queues a `throw warning` on the next tick after `emitWarning`
 *      returns, so a `try`/`catch` around the call would not catch it. We
 *      check the flag up front and route to `console.warn` instead.
 *   2. Runtimes that don't expose `process.emitWarning` at all (older
 *      browsers, certain bundler shims). Same fallback applies.
 *
 * The remaining `try`/`catch` is belt-and-suspenders for non-Node hosts
 * where a polyfilled `emitWarning` could throw synchronously.
 *
 * @param submodule - The legacy v0 submodule being imported (e.g. "workflow", "step").
 * @param detail    - Optional follow-up sentence appended to the warning detail.
 */
export declare function emitV0RemovedWarning(submodule: string, detail?: string): void;
