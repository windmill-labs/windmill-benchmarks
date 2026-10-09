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
var __rest = (this && this.__rest) || function (s, e) {
    var t = {};
    for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0)
        t[p] = s[p];
    if (s != null && typeof Object.getOwnPropertySymbols === "function")
        for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
            if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i]))
                t[p[i]] = s[p[i]];
        }
    return t;
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.WebhooksClient = void 0;
const data_contracts_1 = require("../../../clients/rest/generated/data-contracts");
function getAuthType(auth) {
    if ('username' in auth && 'password' in auth) {
        return data_contracts_1.V1WebhookAuthType.BASIC;
    }
    if ('headerName' in auth && 'apiKey' in auth) {
        return data_contracts_1.V1WebhookAuthType.API_KEY;
    }
    if ('signingSecret' in auth &&
        'signatureHeaderName' in auth &&
        'algorithm' in auth &&
        'encoding' in auth) {
        return data_contracts_1.V1WebhookAuthType.HMAC;
    }
    throw new Error('Invalid webhook auth');
}
function toCreateWebhookRequest(options) {
    const { auth } = options, base = __rest(options, ["auth"]);
    const authType = getAuthType(auth);
    return Object.assign(Object.assign({}, base), { authType, auth });
}
/**
 * Client for managing incoming webhooks in Hatchet.
 *
 * Webhooks allow external systems to trigger Hatchet workflows by sending
 * HTTP requests to dedicated endpoints. This enables real-time integration
 * with third-party services like GitHub, Stripe, Slack, or any system that
 * can send webhook events.
 */
class WebhooksClient {
    constructor(client) {
        this.api = client.api;
        this.tenantId = client.tenantId;
    }
    /**
     * Lists all webhooks for the current tenant.
     * @param options - The options for the list operation.
     * @returns A promise that resolves to the list of webhooks.
     */
    list(options) {
        return __awaiter(this, void 0, void 0, function* () {
            const response = yield this.api.v1WebhookList(this.tenantId, {
                limit: options === null || options === void 0 ? void 0 : options.limit,
                offset: options === null || options === void 0 ? void 0 : options.offset,
                webhookNames: options === null || options === void 0 ? void 0 : options.webhookNames,
                sourceNames: options === null || options === void 0 ? void 0 : options.sourceNames,
            });
            return response.data;
        });
    }
    /**
     * Gets a webhook by its name.
     * @param webhookName - The name of the webhook to get.
     * @returns A promise that resolves to the webhook.
     */
    get(webhookName) {
        return __awaiter(this, void 0, void 0, function* () {
            const response = yield this.api.v1WebhookGet(this.tenantId, webhookName);
            return response.data;
        });
    }
    /**
     * Creates a new webhook.
     * @param request - The request options for the create operation.
     * @returns A promise that resolves to the created webhook.
     */
    create(request) {
        return __awaiter(this, void 0, void 0, function* () {
            const payload = toCreateWebhookRequest(request);
            const response = yield this.api.v1WebhookCreate(this.tenantId, payload);
            return response.data;
        });
    }
    /**
     * Updates a webhook by its name.
     * @param webhookName - The name of the webhook to update.
     * @param options - The options for the update operation.
     * @returns A promise that resolves to the updated webhook.
     */
    update(webhookName_1) {
        return __awaiter(this, arguments, void 0, function* (webhookName, options = {}) {
            const response = yield this.api.v1WebhookUpdate(this.tenantId, webhookName, {
                eventKeyExpression: options.eventKeyExpression,
                scopeExpression: options.scopeExpression,
                staticPayload: options.staticPayload,
            });
            return response.data;
        });
    }
    /**
     * Deletes a webhook by its name.
     * @param webhookName - The name of the webhook to delete.
     * @returns A promise that resolves to the deleted webhook.
     */
    delete(webhookName) {
        return __awaiter(this, void 0, void 0, function* () {
            const response = yield this.api.v1WebhookDelete(this.tenantId, webhookName);
            return response.data;
        });
    }
}
exports.WebhooksClient = WebhooksClient;
