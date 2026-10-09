import { Duration } from '../v1/client/duration';
/**
 * Sleeps for a given number of milliseconds without blocking the event loop
 *
 * WARNING: This is not a durable sleep. It will not be honored if the worker is
 * restarted or crashes.
 *
 * @param duration - The number of milliseconds to sleep, or a Duration (e.g. "5s", \{ seconds: 5 \})
 * @param signal - Optional AbortSignal; if aborted, the promise rejects with Error('Cancelled').
 *                 Use in task handlers so cancellation can interrupt long sleeps.
 * @returns A promise that resolves after the given number of milliseconds (or rejects on abort)
 */
declare function sleep(duration: number | Duration, signal?: AbortSignal): Promise<void>;
export default sleep;
