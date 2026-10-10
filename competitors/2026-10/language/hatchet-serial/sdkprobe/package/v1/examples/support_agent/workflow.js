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
exports.supportAgent = exports.escalateTicket = exports.generateReply = exports.triageTicket = exports.TIMEOUT_SECONDS = exports.REPLY_EVENT_KEY = void 0;
const conditions_1 = require("../../conditions");
const duration_1 = require("../../client/duration");
const hatchet_client_1 = require("../hatchet-client");
exports.REPLY_EVENT_KEY = 'support:customer-reply';
const REPLY_LABEL = 'reply';
const TIMEOUT_LABEL = 'timeout';
exports.TIMEOUT_SECONDS = 5;
const LOOKBACK_WINDOW = '5m';
// !!
// > Triage task
// Classify the ticket into a category and priority.
exports.triageTicket = hatchet_client_1.hatchet.task({
    name: 'triage-ticket',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        const text = `${input.subject} ${input.body}`.toLowerCase();
        let category;
        if (['bill', 'charge', 'payment', 'invoice'].some((w) => text.includes(w))) {
            category = 'billing';
        }
        else if (['login', 'password', 'auth', 'access'].some((w) => text.includes(w))) {
            category = 'account';
        }
        else {
            category = 'technical';
        }
        let priority;
        if (['urgent', 'critical', 'down', 'outage'].some((w) => text.includes(w))) {
            priority = 'high';
        }
        else if (['twice', 'broken', 'error'].some((w) => text.includes(w))) {
            priority = 'medium';
        }
        else {
            priority = 'low';
        }
        return { category, priority };
    }),
});
// !!
// > Generate reply task
// Generate an initial support reply using Claude.
exports.generateReply = hatchet_client_1.hatchet.task({
    name: 'generate-reply',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        const apiKey = process.env.ANTHROPIC_API_KEY;
        if (!apiKey) {
            return {
                message: `Thank you for contacting support about: ${input.subject}. We are looking into this and will get back to you shortly.`,
            };
        }
        // eslint-disable-next-line @typescript-eslint/no-require-imports
        const anthropic = require('@anthropic-ai/sdk');
        const Anthropic = anthropic.default || anthropic;
        const client = new Anthropic({ apiKey });
        const response = yield client.messages.create({
            model: 'claude-sonnet-4-20250514',
            max_tokens: 300,
            messages: [
                {
                    role: 'user',
                    content: `You are a friendly support agent. Write a brief, helpful initial ` +
                        `reply to this support ticket.\n\n` +
                        `Subject: ${input.subject}\n` +
                        `Message: ${input.body}\n\n` +
                        `Keep the reply under 3 sentences.`,
                },
            ],
        });
        const [block] = response.content;
        const text = (block === null || block === void 0 ? void 0 : block.type) === 'text' ? block.text : '';
        return { message: text };
    }),
});
// !!
// > Escalate task
// Escalate an unresolved ticket to the human support team.
exports.escalateTicket = hatchet_client_1.hatchet.task({
    name: 'escalate-ticket',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        return {
            reason: `No customer reply within ${exports.TIMEOUT_SECONDS}s timeout`,
            assignedTo: 'support-team@example.com',
        };
    }),
});
// !!
// > Support agent workflow
exports.supportAgent = hatchet_client_1.hatchet.durableTask({
    name: 'support-agent',
    executionTimeout: '10m',
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        var _a, _b;
        // Step 1: Triage the ticket
        const triage = yield exports.triageTicket.run(input);
        // Step 2: Generate an initial reply
        const reply = yield exports.generateReply.run(input);
        // Step 3: Wait for a customer reply or timeout
        const now = yield ctx.now();
        const considerEventsSince = new Date(now.getTime() - (0, duration_1.durationToMs)(LOOKBACK_WINDOW)).toISOString();
        const waitResult = yield ctx.waitFor((0, conditions_1.Or)(new conditions_1.SleepCondition(`${exports.TIMEOUT_SECONDS}s`, TIMEOUT_LABEL), new conditions_1.UserEventCondition(exports.REPLY_EVENT_KEY, '', REPLY_LABEL, undefined, input.ticketId, considerEventsSince)));
        // Determine which condition fired. ctx.waitFor returns
        // { CREATE: { <label>: ... } } where <label> is the readableDataKey
        // we assigned above ('timeout' or 'reply').
        const create = (_a = waitResult['CREATE']) !== null && _a !== void 0 ? _a : waitResult;
        const resolvedLabel = (_b = Object.keys(create)[0]) !== null && _b !== void 0 ? _b : '';
        const customerReplied = resolvedLabel === REPLY_LABEL;
        if (!customerReplied) {
            // Step 4a: Timeout -> escalate
            yield exports.escalateTicket.run(input);
            return {
                ticketId: input.ticketId,
                status: 'escalated',
                triageCategory: triage.category,
                triagePriority: triage.priority,
                initialReply: reply.message,
            };
        }
        // Step 4b: Customer replied -> resolve
        return {
            ticketId: input.ticketId,
            status: 'resolved',
            triageCategory: triage.category,
            triagePriority: triage.priority,
            initialReply: reply.message,
        };
    }),
});
// !!
