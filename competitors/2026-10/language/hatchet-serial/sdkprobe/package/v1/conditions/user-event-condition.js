"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.UserEventCondition = void 0;
const base_1 = require("./base");
/**
 * Represents a condition that is triggered based on a specific user event.
 * This condition monitors for events with the specified key and evaluates
 * any provided expression against the event data.
 *
 * @param eventKey The key identifying the specific user event to monitor
 * @param expression The CEL expression to evaluate against the event data
 * @param readableDataKey Optional parameter that provides a unique identifier for the data.
 *                        When multiple conditions are listening to the same event, using a custom
 *                        readableDataKey prevents duplicate data by differentiating between the conditions.
 *                        If not provided, defaults to the eventKey.
 * @param action Optional action to execute when the condition is met
 *
 * @example
 * // Create a condition that triggers when a "purchase" event occurs with amount > 100
 * const purchaseCondition = new UserEventCondition(
 *   "purchase",
 *   "data.amount > 100",
 *   "high_value_purchase",
 *   () => console.log("High value purchase detected!")
 * );
 */
class UserEventCondition extends base_1.Condition {
    constructor(eventKey, expression, readableDataKey, action, scope, considerEventsSince) {
        super({
            readableDataKey: readableDataKey || eventKey,
            action,
            orGroupId: '',
            expression: '',
        });
        this.eventKey = eventKey;
        this.expression = expression;
        this.scope = scope;
        this.considerEventsSince = considerEventsSince;
    }
}
exports.UserEventCondition = UserEventCondition;
