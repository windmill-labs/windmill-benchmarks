"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.taskConditionsToPb = taskConditionsToPb;
exports.conditionsToPb = conditionsToPb;
const apply_namespace_1 = require("../../util/apply-namespace");
const _1 = require(".");
const base_1 = require("./base");
const parent_condition_1 = require("./parent-condition");
const duration_1 = require("../client/duration");
function taskConditionsToPb(task, namespace) {
    const waitForConditions = (0, _1.Render)(base_1.Action.QUEUE, task.waitFor);
    const cancelIfConditions = (0, _1.Render)(base_1.Action.CANCEL, task.cancelIf);
    const skipIfConditions = (0, _1.Render)(base_1.Action.SKIP, task.skipIf);
    const mergedConditions = [...waitForConditions, ...cancelIfConditions, ...skipIfConditions];
    return conditionsToPb(mergedConditions, namespace);
}
function conditionsToPb(conditions, namespace) {
    const parentOverrideConditions = [];
    const sleepConditions = [];
    const userEventConditions = [];
    conditions.forEach((condition) => {
        if (condition instanceof _1.SleepCondition) {
            sleepConditions.push({
                base: baseToPb(condition.base),
                sleepFor: (0, duration_1.durationToString)(condition.sleepFor),
            });
        }
        else if (condition instanceof _1.UserEventCondition) {
            userEventConditions.push({
                base: Object.assign(Object.assign({}, baseToPb(condition.base)), { expression: condition.expression || '' }),
                userEventKey: (0, apply_namespace_1.applyNamespace)(condition.eventKey, namespace),
                eventScope: condition.scope,
                considerEventsSince: condition.considerEventsSince
                    ? new Date(condition.considerEventsSince)
                    : undefined,
            });
        }
        else if (condition instanceof parent_condition_1.ParentCondition) {
            parentOverrideConditions.push({
                base: baseToPb(condition.base),
                parentReadableId: condition.parent.name,
            });
        }
    });
    return {
        parentOverrideConditions,
        sleepConditions,
        userEventConditions,
    };
}
function baseToPb(base) {
    return {
        readableDataKey: base.readableDataKey || '',
        action: base.action,
        orGroupId: base.orGroupId || (0, _1.generateGroupId)(),
        expression: base.expression || '',
    };
}
