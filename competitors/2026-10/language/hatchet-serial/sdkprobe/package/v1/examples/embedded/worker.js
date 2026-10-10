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
exports.configuredClient = configuredClient;
exports.fleetClient = fleetClient;
const embedded_1 = require("../../embedded");
function configuredClient() {
    return __awaiter(this, void 0, void 0, function* () {
        // > Configure the embedded engine
        const hatchet = yield embedded_1.HatchetEmbeddedClient.init({
            // use your own Postgres instead of the bundled one
            databaseUrl: 'postgres://...',
            // store the bundled Postgres runtime and data under this directory
            postgresDataDir: '~/my-project/.hatchet-pg',
            // use RabbitMQ instead of the Postgres message queue
            rabbitmqUrl: 'amqp://...',
            // bind the API / gRPC servers to specific ports
            apiPort: 28243,
            grpcPort: 7070,
            // start only the engine + gRPC, no REST API
            startApi: false,
            // skip running migrations on startup
            runMigrations: false,
            // engine log level (default "warn")
            logLevel: 'info',
            // hatchet-embedded release tag to download
            version: 'v0.105.0',
            // use an existing sidecar binary, skips the download
            binaryPath: '/path/to/hatchet-embedded-sidecar',
            // pinned sha256 of the sidecar binary, replaces checksums.txt as the trust anchor
            checksum: '4f2a...',
        });
        // !!
        return hatchet;
    });
}
function fleetClient() {
    return __awaiter(this, void 0, void 0, function* () {
        // > Fleet with a shared database
        const hatchet = yield embedded_1.HatchetEmbeddedClient.init({
            databaseUrl: 'postgres://user:pass@db.internal:5432/hatchet',
        });
        // !!
        return hatchet;
    });
}
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        // > Create an embedded client
        const hatchet = yield embedded_1.HatchetEmbeddedClient.init();
        // !!
        const greet = hatchet.task({
            name: 'embedded-greet',
            fn: (input) => ({ greeting: `Hello, ${input.name}!` }),
        });
        const worker = yield hatchet.worker('embedded-worker', { workflows: [greet] });
        void worker.start();
        const result = yield greet.run({ name: 'embed' });
        console.log(result.greeting);
        // > Stop the embedded engine
        yield hatchet.stopEmbedded();
        // !!
        process.exit(0);
    });
}
if (require.main === module) {
    main();
}
