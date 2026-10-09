import { Condition, Action } from './base';
import { Parent } from './parent-condition';
import { Sleep, SleepCondition } from './sleep-condition';
import { UserEvent, UserEventCondition } from './user-event-condition';
export { Sleep, SleepCondition, UserEvent, UserEventCondition };
export type IConditions = Sleep | UserEvent | Parent;
export type Conditions = Condition | OrCondition | IConditions;
export declare class OrCondition {
    conditions: Condition[];
    constructor(conditions: Condition[]);
}
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
export declare function Render(action?: Action, conditions?: Conditions | Conditions[]): Condition[];
/**
 * Creates a condition group with OR logic
 * Flattens nested Or conditions to ensure proper grouping
 */
export declare function Or(...conditionsOrObjs: (IConditions | Condition)[]): OrCondition;
export declare function generateGroupId(): string;
