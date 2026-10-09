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
exports.CELClient = void 0;
const axios_1 = require("axios");
const data_contracts_1 = require("../../../clients/rest/generated/data-contracts");
/**
 * Client for debugging CEL expressions.
 */
class CELClient {
    constructor(client) {
        this.api = client.api;
        this.tenantId = client.tenantId;
    }
    /**
     * Debug a CEL expression with the provided input, filter payload, and optional metadata. Useful for testing and validating CEL expressions and debugging issues in production.
     *
     * @param input - The input data to evaluate the CEL expression against.
     * @returns A promise that resolves to the evaluation result, which can either be a success with an output or an error with a message.
     * @throws Will throw an error if the input is invalid or the API call fails.
     */
    debug(input) {
        return __awaiter(this, void 0, void 0, function* () {
            var _a;
            try {
                const response = yield this.api.v1CelDebug(this.tenantId, input);
                if (response.data.status === data_contracts_1.V1CELDebugResponseStatus.ERROR) {
                    if (!response.data.error) {
                        throw new Error('No error message received from CEL debug API.');
                    }
                    return {
                        status: data_contracts_1.V1CELDebugResponseStatus.ERROR,
                        error: response.data.error,
                    };
                }
                if (response.data.output === undefined) {
                    throw new Error('No output received from CEL debug API.');
                }
                return {
                    status: data_contracts_1.V1CELDebugResponseStatus.SUCCESS,
                    output: response.data.output,
                };
            }
            catch (err) {
                if (err instanceof axios_1.AxiosError) {
                    throw new Error(JSON.stringify((_a = err.response) === null || _a === void 0 ? void 0 : _a.data.errors), { cause: err });
                }
                throw err;
            }
        });
    }
}
exports.CELClient = CELClient;
