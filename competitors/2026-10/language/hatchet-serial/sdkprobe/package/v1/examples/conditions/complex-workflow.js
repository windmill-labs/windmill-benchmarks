"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.taskConditionWorkflow = void 0;
// > Create a workflow
const conditions_1 = require("../../conditions");
const parent_condition_1 = require("../../conditions/parent-condition");
const hatchet_client_1 = require("../hatchet-client");
exports.taskConditionWorkflow = hatchet_client_1.hatchet.workflow({
    name: 'TaskConditionWorkflow',
});
// !!
// > Add base task
const start = exports.taskConditionWorkflow.task({
    name: 'start',
    fn: () => {
        return {
            randomNumber: Math.floor(Math.random() * 100) + 1,
        };
    },
});
// !!
// > Add wait for sleep
const waitForSleep = exports.taskConditionWorkflow.task({
    name: 'waitForSleep',
    parents: [start],
    waitFor: [new conditions_1.SleepCondition('10s')],
    fn: () => {
        return {
            randomNumber: Math.floor(Math.random() * 100) + 1,
        };
    },
});
// !!
// > Add skip on event
const skipOnEvent = exports.taskConditionWorkflow.task({
    name: 'skipOnEvent',
    parents: [start],
    waitFor: [new conditions_1.SleepCondition('10s')],
    skipIf: [new conditions_1.UserEventCondition('skip_on_event:skip', 'true')],
    fn: () => {
        return {
            randomNumber: Math.floor(Math.random() * 100) + 1,
        };
    },
});
// !!
// > Add branching
const leftBranch = exports.taskConditionWorkflow.task({
    name: 'leftBranch',
    parents: [waitForSleep],
    skipIf: [new parent_condition_1.ParentCondition(waitForSleep, 'output.randomNumber > 50')],
    fn: () => {
        return {
            randomNumber: Math.floor(Math.random() * 100) + 1,
        };
    },
});
const rightBranch = exports.taskConditionWorkflow.task({
    name: 'rightBranch',
    parents: [waitForSleep],
    skipIf: [new parent_condition_1.ParentCondition(waitForSleep, 'output.randomNumber <= 50')],
    fn: () => {
        return {
            randomNumber: Math.floor(Math.random() * 100) + 1,
        };
    },
});
// !!
// > Add wait for event
const waitForEvent = exports.taskConditionWorkflow.task({
    name: 'waitForEvent',
    parents: [start],
    waitFor: [(0, conditions_1.Or)(new conditions_1.SleepCondition('1m'), new conditions_1.UserEventCondition('wait_for_event:start', 'true'))],
    fn: () => {
        return {
            randomNumber: Math.floor(Math.random() * 100) + 1,
        };
    },
});
// !!
// > Add sum
exports.taskConditionWorkflow.task({
    name: 'sum',
    parents: [start, waitForSleep, waitForEvent, skipOnEvent, leftBranch, rightBranch],
    fn: (_, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        var _a, _b, _c;
        const one = (yield ctx.parentOutput(start)).randomNumber;
        const two = (yield ctx.parentOutput(waitForEvent)).randomNumber;
        const three = (yield ctx.parentOutput(waitForSleep)).randomNumber;
        const four = ((_a = (yield ctx.parentOutput(skipOnEvent))) === null || _a === void 0 ? void 0 : _a.randomNumber) || 0;
        const five = ((_b = (yield ctx.parentOutput(leftBranch))) === null || _b === void 0 ? void 0 : _b.randomNumber) || 0;
        const six = ((_c = (yield ctx.parentOutput(rightBranch))) === null || _c === void 0 ? void 0 : _c.randomNumber) || 0;
        return {
            sum: one + two + three + four + five + six,
        };
    }),
});
// !!
