"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.SleepCondition = void 0;
const duration_1 = require("../client/duration");
const base_1 = require("./base");
/**
 * Represents a condition that waits for a specified duration before proceeding.
 * This condition is useful for implementing time delays in workflows.
 *
 * @example
 * // Create a condition that waits for 5 minutes
 * const waitCondition = new SleepCondition(
 *   "5m",
 *   "reminder_delay",
 *   () => console.log("Wait period completed!")
 * );
 */
class SleepCondition extends base_1.Condition {
    /**
     * Creates a new sleep condition that waits for the specified duration.
     *
     * @param sleepFor Duration to wait in Go duration string format (e.g., "30s", "5m")
     * @param readableDataKey Optional unique identifier for the condition data.
     *                        When multiple sleep conditions have the same duration, using a custom
     *                        readableDataKey prevents duplicate data by differentiating between them.
     *                        If not provided, defaults to `sleep-${sleepFor}`.
     * @param action Optional action to execute when the sleep duration completes
     */
    constructor(sleepFor, readableDataKey, action) {
        super({
            readableDataKey: readableDataKey || `sleep-${(0, duration_1.durationToString)(sleepFor)}`,
            action,
            orGroupId: '',
            expression: '',
        });
        this.sleepFor = sleepFor;
    }
}
exports.SleepCondition = SleepCondition;
