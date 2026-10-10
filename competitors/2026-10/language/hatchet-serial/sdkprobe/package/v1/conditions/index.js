"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.OrCondition = exports.UserEventCondition = exports.SleepCondition = void 0;
exports.Render = Render;
exports.Or = Or;
exports.generateGroupId = generateGroupId;
const base_1 = require("./base");
const parent_condition_1 = require("./parent-condition");
const sleep_condition_1 = require("./sleep-condition");
Object.defineProperty(exports, "SleepCondition", { enumerable: true, get: function () { return sleep_condition_1.SleepCondition; } });
const user_event_condition_1 = require("./user-event-condition");
Object.defineProperty(exports, "UserEventCondition", { enumerable: true, get: function () { return user_event_condition_1.UserEventCondition; } });
class OrCondition {
    constructor(conditions) {
        this.conditions = conditions;
    }
}
exports.OrCondition = OrCondition;
/**
 * Creates a condition that waits for all provided conditions to be met (AND logic)
 * use Or() to create a condition that waits for any of the provided conditions to be met (OR logic)
 * @param conditions - Conditions or OrConditions to be rendered
 * @returns A flattened array of Conditions
 *
 * @example
 * const conditions = Render(
 *   Or({ sleepFor: 5 }, { eventKey: 'user:update' }),
 *   { eventKey: 'user:create' },
 *   Or({ eventKey: 'user:update' }, { eventKey: 'user:delete' })
 * );
 */
function Render(action, conditions) {
    if (!conditions) {
        return [];
    }
    if (!Array.isArray(conditions)) {
        return Render(action, [conditions]);
    }
    const renderedConditions = conditions.reduce((acc, conditionOrObj) => {
        if (conditionOrObj instanceof base_1.Condition) {
            return [...acc, conditionOrObj];
        }
        if (conditionOrObj instanceof OrCondition) {
            return [...acc, ...Render(action, conditionOrObj.conditions)];
        }
        // Handle object syntax
        if ('sleepFor' in conditionOrObj) {
            return [...acc, new sleep_condition_1.SleepCondition(conditionOrObj.sleepFor, undefined, action)];
        }
        if ('eventKey' in conditionOrObj) {
            return [
                ...acc,
                new user_event_condition_1.UserEventCondition(conditionOrObj.eventKey, conditionOrObj.expression || '', undefined, action, conditionOrObj.scope, conditionOrObj.considerEventsSince),
            ];
        }
        if ('parent' in conditionOrObj) {
            return [
                ...acc,
                new parent_condition_1.ParentCondition(conditionOrObj.parent, conditionOrObj.expression || '', undefined, action),
            ];
        }
        throw new Error(`Unknown condition object: ${JSON.stringify(conditionOrObj)}`);
    }, []);
    // set the action for each condition
    return renderedConditions.filter((condition) => {
        if (condition instanceof base_1.Condition) {
            condition.base.action = action;
        }
        return condition;
    });
}
/**
 * Creates a condition group with OR logic
 * Flattens nested Or conditions to ensure proper grouping
 */
function Or(...conditionsOrObjs) {
    // must be Condition[] because OrCondition is not a Condition
    const conditions = Render(undefined, conditionsOrObjs);
    const orGroupId = generateGroupId();
    conditions.forEach((condition) => {
        if (condition instanceof base_1.Condition) {
            condition.base.orGroupId = orGroupId;
        }
    });
    return new OrCondition(conditions);
}
function generateGroupId() {
    return crypto.randomUUID ? crypto.randomUUID() : `or-${Date.now()}-${Math.random()}`;
}
