"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const https_1 = __importDefault(require("https"));
const qs_1 = __importDefault(require("qs"));
const Api_1 = require("./generated/Api");
function getDefaultAxiosOpts(serverUrl) {
    const opts = {};
    if (serverUrl.startsWith('https://') && process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') {
        opts.httpsAgent = new https_1.default.Agent({ rejectUnauthorized: false });
    }
    return opts;
}
const api = (serverUrl, token, axiosOpts) => {
    return new Api_1.Api(Object.assign(Object.assign({ baseURL: serverUrl, headers: {
            Authorization: `Bearer ${token}`,
        }, paramsSerializer: (params) => qs_1.default.stringify(params, { arrayFormat: 'repeat' }) }, getDefaultAxiosOpts(serverUrl)), axiosOpts));
};
exports.default = api;
