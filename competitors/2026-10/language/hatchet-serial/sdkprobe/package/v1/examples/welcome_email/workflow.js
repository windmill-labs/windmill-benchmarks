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
exports.welcomeEmail = exports.ONBOARDING_EVENT_KEY = void 0;
const conditions_1 = require("../../conditions");
const duration_1 = require("../../client/duration");
const hatchet_client_1 = require("../hatchet-client");
exports.ONBOARDING_EVENT_KEY = 'user:onboarding-completed';
const TIMEOUT_SECONDS = 5;
const LOOKBACK_WINDOW = '5m';
// !!
// > Welcome email task
exports.welcomeEmail = hatchet_client_1.hatchet.durableTask({
    name: 'welcome-email',
    onEvents: ['user:signup'],
    executionTimeout: '5m',
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        var _a, _b;
        // Step 1: Send the welcome email
        ctx.logger.info(`Sending welcome email to ${input.email}: finish your first onboarding step`);
        // Step 2: Wait for the user to complete onboarding, or time out
        // (use a longer duration for a more realistic workflow)
        const now = yield ctx.now();
        const considerEventsSince = new Date(now.getTime() - (0, duration_1.durationToMs)(LOOKBACK_WINDOW)).toISOString();
        const waitResult = yield ctx.waitFor((0, conditions_1.Or)({ sleepFor: `${TIMEOUT_SECONDS}s` }, 
        // Scope the event condition to this user so that another user's
        // onboarding-completed event does not resolve this wait.
        { eventKey: exports.ONBOARDING_EVENT_KEY, scope: input.user_id, considerEventsSince }));
        // The or-group result is { CREATE: { <condition_key>: ... } }.
        // Check whether the onboarding event was the one that resolved.
        const create = (_a = waitResult['CREATE']) !== null && _a !== void 0 ? _a : waitResult;
        const resolvedKey = (_b = Object.keys(create)[0]) !== null && _b !== void 0 ? _b : '';
        const onboardingCompleted = resolvedKey === exports.ONBOARDING_EVENT_KEY;
        if (onboardingCompleted) {
            // Step 3a: User completed onboarding -> skip follow-up
            ctx.logger.info(`User ${input.user_id} completed onboarding, skipping follow-up`);
            return { userId: input.user_id, welcomeSent: true, followUpSent: false };
        }
        // Step 3b: Timeout -> send follow-up email
        ctx.logger.info(`Sending follow-up email to ${input.email}: need help finishing onboarding?`);
        return { userId: input.user_id, welcomeSent: true, followUpSent: true };
    }),
});
// !!
