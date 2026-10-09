/** Canonical reasons when cancelling a HatchetPromise (e.g. worker shutdown). */
export declare enum CancellationReason {
    CANCELLED_BY_WORKER = "Cancelled by worker",
    EVICTED_BY_WORKER = "Evicted by worker"
}
declare class HatchetPromise<T> {
    cancel: (reason?: CancellationReason) => void;
    promise: Promise<T>;
    /**
     * The original (non-cancelable) promise passed to the constructor.
     *
     * `promise` is a cancelable wrapper which rejects immediately when `cancel` is called.
     * `inner` continues executing and will settle when the underlying work completes.
     */
    inner: Promise<T>;
    constructor(promise: Promise<T>);
}
export default HatchetPromise;
