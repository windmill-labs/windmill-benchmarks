"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ParentCondition = void 0;
const base_1 = require("./base");
/**
 * Represents a condition that is triggered based on a parent workflow task.
 * This condition monitors the specified parent task and evaluates
 * any provided expression against the task's data.
 *
 * @example
 * // Create a condition that triggers when a parent task completes successfully
 * const parentCondition = new ParentCondition(
 *   parentTaskOpts,
 *   "input.status == 'success'",
 *   () => console.log("Parent task completed successfully!")
 * );
 */
class ParentCondition extends base_1.Condition {
    /**
     * Creates a new condition that is triggered based on a parent workflow task.
     *
     * @param parent The parent workflow task this condition is associated with
     * @param expression Optional CEL expression to evaluate against the parent task's data
     * @param action Optional action to execute when the condition is met
     */
    constructor(parent, expression, readableDataKey, action) {
        super({
            readableDataKey: readableDataKey || `parent-${parent.name || Date.now().toString()}`,
            action,
            orGroupId: '',
            expression: expression || '',
        });
        this.parent = parent;
    }
}
exports.ParentCondition = ParentCondition;
