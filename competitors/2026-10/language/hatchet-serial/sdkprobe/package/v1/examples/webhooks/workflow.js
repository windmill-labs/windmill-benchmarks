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
exports.handleSlackInteraction = exports.handleSlackCommand = exports.handleSlackMention = exports.handleGitHubPR = exports.handleStripePayment = exports.webhookWorkflow = void 0;
const hatchet_client_1 = require("../hatchet-client");
exports.webhookWorkflow = hatchet_client_1.hatchet.workflow({
    name: 'webhook-workflow',
    onEvents: ['webhook:test'],
});
exports.webhookWorkflow.task({
    name: 'webhook-task',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        return input;
    }),
});
exports.handleStripePayment = hatchet_client_1.hatchet.task({
    name: 'handle-stripe-payment',
    on: {
        event: 'stripe:payment_intent.succeeded',
    },
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const { customer, amount } = input.data.object;
        ctx.logger.info(`Payment of ${amount} from ${customer}`);
        return { customer, amount };
    }),
});
exports.handleGitHubPR = hatchet_client_1.hatchet.task({
    name: 'handle-github-pr',
    on: {
        event: 'github:pull_request:opened',
    },
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const repo = input.repository.full_name;
        const prNumber = input.pull_request.number;
        const { title } = input.pull_request;
        ctx.logger.info(`PR #${prNumber} opened on ${repo}: ${title}`);
        return { repo, pr: prNumber };
    }),
});
exports.handleSlackMention = hatchet_client_1.hatchet.task({
    name: 'handle-slack-mention',
    on: {
        event: 'slack:event:app_mention',
    },
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const { user, text, channel } = input.event;
        ctx.logger.info(`Mentioned by ${user} in ${channel}: ${text}`);
        return { handled: true };
    }),
});
exports.handleSlackCommand = hatchet_client_1.hatchet.task({
    name: 'handle-slack-command',
    on: {
        event: 'slack:command:/deploy',
    },
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        ctx.logger.info(`${input.user_name} ran ${input.command} ${input.text}`);
        return { command: input.command, args: input.text };
    }),
});
exports.handleSlackInteraction = hatchet_client_1.hatchet.task({
    name: 'handle-slack-interaction',
    on: {
        event: 'slack:interaction:block_actions',
    },
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const [action] = input.actions;
        ctx.logger.info(`${input.user.username} clicked button: ${action.action_id}`);
        return { action: action.action_id };
    }),
});
// !!
