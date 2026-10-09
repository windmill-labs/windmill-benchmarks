"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.ConfigLoader = void 0;
const yaml_1 = require("yaml");
const fs_1 = require("fs");
const p = __importStar(require("path"));
const v4_1 = require("zod/v4");
const hatchet_client_1 = require("../../clients/hatchet-client");
const nice_grpc_1 = require("nice-grpc");
const token_1 = require("./token");
const DEFAULT_CONFIG_FILE = '.hatchet.yaml';
class ConfigLoader {
    static loadClientConfig(override, config) {
        var _a, _b, _c, _d, _e, _f, _g, _h, _j, _k, _l, _m, _o, _p, _q, _r, _s, _t, _u, _v, _w, _x, _y, _z, _0, _1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16, _17, _18, _19, _20, _21, _22, _23, _24, _25, _26, _27, _28, _29, _30;
        const yaml = this.loadYamlConfig(config === null || config === void 0 ? void 0 : config.path);
        const tlsConfig = (_a = override === null || override === void 0 ? void 0 : override.tls_config) !== null && _a !== void 0 ? _a : {
            tls_strategy: (_d = (_c = (_b = yaml === null || yaml === void 0 ? void 0 : yaml.tls_config) === null || _b === void 0 ? void 0 : _b.tls_strategy) !== null && _c !== void 0 ? _c : this.env('HATCHET_CLIENT_TLS_STRATEGY')) !== null && _d !== void 0 ? _d : 'tls',
            cert_file: (_f = (_e = yaml === null || yaml === void 0 ? void 0 : yaml.tls_config) === null || _e === void 0 ? void 0 : _e.cert_file) !== null && _f !== void 0 ? _f : this.env('HATCHET_CLIENT_TLS_CERT_FILE'),
            key_file: (_h = (_g = yaml === null || yaml === void 0 ? void 0 : yaml.tls_config) === null || _g === void 0 ? void 0 : _g.key_file) !== null && _h !== void 0 ? _h : this.env('HATCHET_CLIENT_TLS_KEY_FILE'),
            ca_file: (_k = (_j = yaml === null || yaml === void 0 ? void 0 : yaml.tls_config) === null || _j === void 0 ? void 0 : _j.ca_file) !== null && _k !== void 0 ? _k : this.env('HATCHET_CLIENT_TLS_ROOT_CA_FILE'),
            server_name: (_m = (_l = yaml === null || yaml === void 0 ? void 0 : yaml.tls_config) === null || _l === void 0 ? void 0 : _l.server_name) !== null && _m !== void 0 ? _m : this.env('HATCHET_CLIENT_TLS_SERVER_NAME'),
        };
        const token = (_p = (_o = override === null || override === void 0 ? void 0 : override.token) !== null && _o !== void 0 ? _o : yaml === null || yaml === void 0 ? void 0 : yaml.token) !== null && _p !== void 0 ? _p : this.env('HATCHET_CLIENT_TOKEN');
        const healthCheckConfig = (_r = (_q = override === null || override === void 0 ? void 0 : override.healthcheck) !== null && _q !== void 0 ? _q : yaml === null || yaml === void 0 ? void 0 : yaml.healthcheck) !== null && _r !== void 0 ? _r : {
            enabled: this.env('HATCHET_CLIENT_WORKER_HEALTHCHECK_ENABLED') === 'true',
            port: parseInt(this.env('HATCHET_CLIENT_WORKER_HEALTHCHECK_PORT') || '8001', 10),
        };
        if (!token) {
            throw new Error('API token is required. Set it via the HATCHET_CLIENT_TOKEN environment variable. For local development, you can run Hatchet embedded via HatchetEmbeddedClient.init() (imported from @hatchet-dev/typescript-sdk/v1/embedded). More docs here: https://docs.hatchet.run/v1/embedded');
        }
        let grpcBroadcastAddress;
        let apiUrl;
        const tenantId = (0, token_1.getTenantIdFromJWT)(token);
        if (!tenantId) {
            throw new Error('Tenant ID not found in subject claim of token');
        }
        try {
            const addresses = (0, token_1.getAddressesFromJWT)(token);
            grpcBroadcastAddress =
                (_u = (_t = (_s = override === null || override === void 0 ? void 0 : override.host_port) !== null && _s !== void 0 ? _s : yaml === null || yaml === void 0 ? void 0 : yaml.host_port) !== null && _t !== void 0 ? _t : this.env('HATCHET_CLIENT_HOST_PORT')) !== null && _u !== void 0 ? _u : addresses.grpcBroadcastAddress;
            apiUrl =
                (_x = (_w = (_v = override === null || override === void 0 ? void 0 : override.api_url) !== null && _v !== void 0 ? _v : yaml === null || yaml === void 0 ? void 0 : yaml.api_url) !== null && _w !== void 0 ? _w : this.env('HATCHET_CLIENT_API_URL')) !== null && _x !== void 0 ? _x : addresses.serverUrl;
        }
        catch (_31) {
            grpcBroadcastAddress =
                (_z = (_y = override === null || override === void 0 ? void 0 : override.host_port) !== null && _y !== void 0 ? _y : yaml === null || yaml === void 0 ? void 0 : yaml.host_port) !== null && _z !== void 0 ? _z : this.env('HATCHET_CLIENT_HOST_PORT');
            apiUrl = (_1 = (_0 = override === null || override === void 0 ? void 0 : override.api_url) !== null && _0 !== void 0 ? _0 : yaml === null || yaml === void 0 ? void 0 : yaml.api_url) !== null && _1 !== void 0 ? _1 : this.env('HATCHET_CLIENT_API_URL');
        }
        let namespace = (_3 = (_2 = override === null || override === void 0 ? void 0 : override.namespace) !== null && _2 !== void 0 ? _2 : yaml === null || yaml === void 0 ? void 0 : yaml.namespace) !== null && _3 !== void 0 ? _3 : this.env('HATCHET_CLIENT_NAMESPACE');
        if (namespace && !(namespace === null || namespace === void 0 ? void 0 : namespace.endsWith('_'))) {
            namespace = `${namespace}_`;
        }
        const otelConfig = (_5 = (_4 = override === null || override === void 0 ? void 0 : override.otel) !== null && _4 !== void 0 ? _4 : yaml === null || yaml === void 0 ? void 0 : yaml.otel) !== null && _5 !== void 0 ? _5 : {
            excludedAttributes: this.parseJsonArray(this.env('HATCHET_CLIENT_OPENTELEMETRY_EXCLUDED_ATTRIBUTES') || '[]'),
            includeTaskNameInSpanName: this.env('HATCHET_CLIENT_OPENTELEMETRY_INCLUDE_TASK_NAME_IN_SPAN_NAME') === 'true',
            individualRunSpansForBulkRun: this.env('HATCHET_CLIENT_OPENTELEMETRY_INDIVIDUAL_RUN_SPANS_FOR_BULK_RUN') === 'true',
        };
        const grpcMaxRecvMessageLength = (_8 = (_7 = (_6 = override === null || override === void 0 ? void 0 : override.grpc_max_recv_message_length) !== null && _6 !== void 0 ? _6 : yaml === null || yaml === void 0 ? void 0 : yaml.grpc_max_recv_message_length) !== null && _7 !== void 0 ? _7 : this.parseIntEnv('HATCHET_CLIENT_GRPC_MAX_RECV_MESSAGE_LENGTH')) !== null && _8 !== void 0 ? _8 : 4 * 1024 * 1024;
        const grpcMaxSendMessageLength = (_11 = (_10 = (_9 = override === null || override === void 0 ? void 0 : override.grpc_max_send_message_length) !== null && _9 !== void 0 ? _9 : yaml === null || yaml === void 0 ? void 0 : yaml.grpc_max_send_message_length) !== null && _10 !== void 0 ? _10 : this.parseIntEnv('HATCHET_CLIENT_GRPC_MAX_SEND_MESSAGE_LENGTH')) !== null && _11 !== void 0 ? _11 : 4 * 1024 * 1024;
        const retrierConfig = {
            maxAttempts: (_15 = (_13 = (_12 = override === null || override === void 0 ? void 0 : override.retrier) === null || _12 === void 0 ? void 0 : _12.maxAttempts) !== null && _13 !== void 0 ? _13 : (_14 = yaml === null || yaml === void 0 ? void 0 : yaml.retrier) === null || _14 === void 0 ? void 0 : _14.maxAttempts) !== null && _15 !== void 0 ? _15 : this.parseIntEnv('HATCHET_CLIENT_RETRIER_MAX_ATTEMPTS'),
            initialInterval: (_19 = (_17 = (_16 = override === null || override === void 0 ? void 0 : override.retrier) === null || _16 === void 0 ? void 0 : _16.initialInterval) !== null && _17 !== void 0 ? _17 : (_18 = yaml === null || yaml === void 0 ? void 0 : yaml.retrier) === null || _18 === void 0 ? void 0 : _18.initialInterval) !== null && _19 !== void 0 ? _19 : this.parseFloatEnv('HATCHET_CLIENT_RETRIER_INITIAL_INTERVAL'),
            maxJitter: (_23 = (_21 = (_20 = override === null || override === void 0 ? void 0 : override.retrier) === null || _20 === void 0 ? void 0 : _20.maxJitter) !== null && _21 !== void 0 ? _21 : (_22 = yaml === null || yaml === void 0 ? void 0 : yaml.retrier) === null || _22 === void 0 ? void 0 : _22.maxJitter) !== null && _23 !== void 0 ? _23 : this.parseIntEnv('HATCHET_CLIENT_RETRIER_MAX_JITTER'),
        };
        const hasRetrier = Object.values(retrierConfig).some((v) => v !== undefined);
        return Object.assign({ token: (_25 = (_24 = override === null || override === void 0 ? void 0 : override.token) !== null && _24 !== void 0 ? _24 : yaml === null || yaml === void 0 ? void 0 : yaml.token) !== null && _25 !== void 0 ? _25 : this.env('HATCHET_CLIENT_TOKEN'), host_port: grpcBroadcastAddress, api_url: apiUrl, tls_config: tlsConfig, healthcheck: healthCheckConfig, log_level: (_28 = (_27 = (_26 = override === null || override === void 0 ? void 0 : override.log_level) !== null && _26 !== void 0 ? _26 : yaml === null || yaml === void 0 ? void 0 : yaml.log_level) !== null && _27 !== void 0 ? _27 : this.env('HATCHET_CLIENT_LOG_LEVEL')) !== null && _28 !== void 0 ? _28 : 'INFO', tenant_id: tenantId, namespace: namespace ? `${namespace}`.toLowerCase() : '', otel: otelConfig, grpc_max_recv_message_length: grpcMaxRecvMessageLength, grpc_max_send_message_length: grpcMaxSendMessageLength, cancellation_grace_period: (_29 = override === null || override === void 0 ? void 0 : override.cancellation_grace_period) !== null && _29 !== void 0 ? _29 : yaml === null || yaml === void 0 ? void 0 : yaml.cancellation_grace_period, cancellation_warning_threshold: (_30 = override === null || override === void 0 ? void 0 : override.cancellation_warning_threshold) !== null && _30 !== void 0 ? _30 : yaml === null || yaml === void 0 ? void 0 : yaml.cancellation_warning_threshold }, (hasRetrier ? { retrier: retrierConfig } : {}));
    }
    static parseIntEnv(envName) {
        const value = this.env(envName);
        if (value === undefined || value === '')
            return undefined;
        if (!/^\d+$/.test(value.trim())) {
            throw new Error(`Invalid value for ${envName}: "${value}". Expected a positive integer.`);
        }
        return parseInt(value, 10);
    }
    static parseFloatEnv(envName) {
        const value = this.env(envName);
        if (value === undefined || value === '')
            return undefined;
        const parsed = parseFloat(value.trim());
        if (isNaN(parsed) || parsed <= 0) {
            throw new Error(`Invalid value for ${envName}: "${value}". Expected a positive number.`);
        }
        return parsed;
    }
    static parseJsonArray(value) {
        try {
            const parsed = JSON.parse(value);
            return Array.isArray(parsed) ? parsed : [];
        }
        catch (_a) {
            return [];
        }
    }
    static get default_yaml_config_path() {
        return p.join(process.cwd(), DEFAULT_CONFIG_FILE);
    }
    static createCredentials(config) {
        // if none, create insecure credentials
        if (config.tls_strategy === 'none') {
            return nice_grpc_1.ChannelCredentials.createInsecure();
        }
        if (config.tls_strategy === 'tls') {
            const rootCerts = config.ca_file ? (0, fs_1.readFileSync)(config.ca_file) : undefined;
            return nice_grpc_1.ChannelCredentials.createSsl(rootCerts);
        }
        const rootCerts = config.ca_file ? (0, fs_1.readFileSync)(config.ca_file) : null;
        const privateKey = config.key_file ? (0, fs_1.readFileSync)(config.key_file) : null;
        const certChain = config.cert_file ? (0, fs_1.readFileSync)(config.cert_file) : null;
        return nice_grpc_1.ChannelCredentials.createSsl(rootCerts, privateKey, certChain);
    }
    static loadYamlConfig(path) {
        try {
            const configFile = (0, fs_1.readFileSync)(p.join(__dirname, path !== null && path !== void 0 ? path : this.default_yaml_config_path), 'utf8');
            const config = (0, yaml_1.parse)(configFile);
            hatchet_client_1.ClientConfigSchema.partial().parse(config);
            return config;
        }
        catch (e) {
            if (!path) {
                return undefined;
            }
            if (e instanceof v4_1.z.ZodError) {
                throw new Error(`Invalid yaml config: ${e.message}`, { cause: e });
            }
            throw e;
        }
    }
    static env(name) {
        return process.env[name];
    }
}
exports.ConfigLoader = ConfigLoader;
