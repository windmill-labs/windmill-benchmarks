"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.LegacyHatchetClient = void 0;
const v4_1 = require("zod/v4");
const config_loader_1 = require("../util/config-loader");
const event_client_1 = require("../clients/event/event-client");
const dispatcher_client_1 = require("../clients/dispatcher/dispatcher-client");
const admin_client_1 = require("../clients/admin/admin-client");
const nice_grpc_1 = require("nice-grpc");
const hatchet_logger_1 = require("../clients/hatchet-client/hatchet-logger");
const grpc_helpers_1 = require("../util/grpc-helpers");
const client_config_1 = require("../clients/hatchet-client/client-config");
const child_listener_client_1 = require("../clients/listeners/run-listener/child-listener-client");
const rest_1 = __importDefault(require("../clients/rest"));
const durable_listener_client_1 = require("../clients/listeners/durable-listener/durable-listener-client");
const transport_1 = require("../clients/transport");
class LegacyHatchetClient {
    constructor(config, options, axiosOpts, runs, listener, event, dispatcher, logger, durableListener) {
        // Initializes a new Client instance.
        // Loads config in the following order: config param > yaml file > env vars
        var _a, _b;
        this.runs = runs;
        const loaded = config_loader_1.ConfigLoader.loadClientConfig(config, {
            path: options === null || options === void 0 ? void 0 : options.config_path,
        });
        try {
            const valid = client_config_1.ClientConfigSchema.parse(loaded);
            let logConstructor = config === null || config === void 0 ? void 0 : config.logger;
            if (logConstructor == null) {
                logConstructor = hatchet_logger_1.DEFAULT_LOGGER;
            }
            this.config = Object.assign(Object.assign({}, valid), { logger: logConstructor });
        }
        catch (e) {
            if (e instanceof v4_1.z.ZodError) {
                throw new Error(`Invalid client config: ${e.message}`, { cause: e });
            }
            throw e;
        }
        this.credentials =
            (_a = options === null || options === void 0 ? void 0 : options.credentials) !== null && _a !== void 0 ? _a : config_loader_1.ConfigLoader.createCredentials(this.config.tls_config);
        const clientFactory = (0, nice_grpc_1.createClientFactory)().use((0, grpc_helpers_1.addTokenMiddleware)(this.config.token));
        const transport = (_b = options === null || options === void 0 ? void 0 : options.transport) !== null && _b !== void 0 ? _b : (0, transport_1.createNodeTransport)(this.config);
        this.tenantId = this.config.tenant_id;
        this.api = (0, rest_1.default)(this.config.api_url, this.config.token, axiosOpts);
        this.event =
            event ||
                new event_client_1.EventClient(this.config, (0, grpc_helpers_1.channelFactory)(this.config, this.credentials), clientFactory, this.api, transport);
        this.dispatcher =
            dispatcher ||
                new dispatcher_client_1.DispatcherClient(this.config, (0, grpc_helpers_1.channelFactory)(this.config, this.credentials), clientFactory);
        this.listener =
            listener ||
                new child_listener_client_1.RunListenerClient(this.config, (0, grpc_helpers_1.channelFactory)(this.config, this.credentials), clientFactory, this.api);
        this.admin = new admin_client_1.AdminClient(this.config, (0, grpc_helpers_1.channelFactory)(this.config, this.credentials), clientFactory, this.api, this.tenantId, this.listener, this.runs, transport);
        this.durableListener =
            durableListener ||
                new durable_listener_client_1.DurableListenerClient(this.config, (0, grpc_helpers_1.channelFactory)(this.config, this.credentials), clientFactory);
        this.logger = logger || this.config.logger('HatchetClient', this.config.log_level);
        this.logger.debug(`Initialized HatchetClient`);
    }
    static init(config, options, axiosConfig) {
        return new LegacyHatchetClient(config, options, axiosConfig);
    }
}
exports.LegacyHatchetClient = LegacyHatchetClient;
