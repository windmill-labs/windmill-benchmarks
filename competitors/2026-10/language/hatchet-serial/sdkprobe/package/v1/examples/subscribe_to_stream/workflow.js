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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.dagStream = exports.longStream = exports.INTER_CHUNK_MS = exports.PRE_STREAM_SLEEP_MS = exports.STREAM_CHUNKS = void 0;
const sleep_1 = __importDefault(require("../../../util/sleep"));
const hatchet_client_1 = require("../hatchet-client");
exports.STREAM_CHUNKS = Array.from({ length: 20 }, (_, i) => `c${String(i).padStart(2, '0')}|`);
exports.PRE_STREAM_SLEEP_MS = 2000;
exports.INTER_CHUNK_MS = 50;
exports.longStream = hatchet_client_1.hatchet.task({
    name: 'subscribe-stream-long',
    fn: (_, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        yield (0, sleep_1.default)(exports.PRE_STREAM_SLEEP_MS);
        for (const chunk of exports.STREAM_CHUNKS) {
            yield ctx.putStream(chunk);
            yield (0, sleep_1.default)(exports.INTER_CHUNK_MS);
        }
    }),
});
// An onFailure handler makes the run a DAG. DAG QUEUED->RUNNING used to be
// published as workflow-run-finished, which hung up stream subscribers as
// soon as the run started.
exports.dagStream = hatchet_client_1.hatchet.workflow({
    name: 'subscribe-stream-dag',
});
exports.dagStream.task({
    name: 'subscribe-stream-dag',
    fn: (_, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        yield (0, sleep_1.default)(exports.PRE_STREAM_SLEEP_MS);
        for (const chunk of exports.STREAM_CHUNKS) {
            yield ctx.putStream(chunk);
        }
    }),
});
exports.dagStream.onFailure({
    name: 'subscribe-stream-dag-on-failure',
    fn: () => __awaiter(void 0, void 0, void 0, function* () { }),
});
