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
exports.batchChildBatchSpawn = exports.batchChildSpawn = exports.childBatch = exports.child = exports.batchCancel = exports.batchBroadcast = exports.batchOrdered = exports.batchSingle = exports.batchLarge = exports.batchKeyedInterval = exports.batchKeyedFailable = exports.batchKeyed = exports.batchSimple = void 0;
const hatchet_client_1 = require("../hatchet-client");
// > Declaring a batch task
exports.batchSimple = hatchet_client_1.hatchet.batchTask({
    name: 'batch-simple',
    batch: { maxSize: 3, maxInterval: 200 },
    fn: (tasks) => __awaiter(void 0, void 0, void 0, function* () {
        const out = {};
        Object.entries(tasks).forEach(([id, input]) => {
            out[id] = { transformed_message: input.message.toUpperCase() };
        });
        return out;
    }),
});
// !!
// > Declaring a keyed batch task
exports.batchKeyed = hatchet_client_1.hatchet.batchTask({
    name: 'batch-keyed',
    batch: { maxSize: 2, maxInterval: 200, groupKey: 'input.group' },
    fn: (tasks) => __awaiter(void 0, void 0, void 0, function* () {
        const uniqueKeys = new Set(Object.values(tasks).map((i) => i.group)).size;
        const batchSize = Object.keys(tasks).length;
        const out = {};
        Object.entries(tasks).forEach(([id, input]) => {
            out[id] = {
                batch_key: input.group,
                batch_size: batchSize,
                unique_keys: uniqueKeys,
                uppercase: input.message.toUpperCase(),
            };
        });
        return out;
    }),
});
// !!
exports.batchKeyedFailable = hatchet_client_1.hatchet.batchTask({
    name: 'batch-keyed-failable',
    batch: { maxSize: 2, maxInterval: 200, groupKey: 'input.group' },
    fn: (tasks) => __awaiter(void 0, void 0, void 0, function* () {
        const out = {};
        Object.entries(tasks).forEach(([id, input]) => {
            out[id] = { uppercase: input.message.toUpperCase() };
        });
        return out;
    }),
});
exports.batchKeyedInterval = hatchet_client_1.hatchet.batchTask({
    name: 'batch-keyed-interval',
    batch: { maxSize: 3, maxInterval: 150, groupKey: 'input.group' },
    fn: (tasks) => __awaiter(void 0, void 0, void 0, function* () {
        const uniqueKeys = new Set(Object.values(tasks).map((i) => i.group)).size;
        const batchSize = Object.keys(tasks).length;
        const out = {};
        Object.entries(tasks).forEach(([id, input]) => {
            out[id] = {
                batch_key: input.group,
                batch_size: batchSize,
                unique_keys: uniqueKeys,
                uppercase: input.message.toUpperCase(),
            };
        });
        return out;
    }),
});
exports.batchLarge = hatchet_client_1.hatchet.batchTask({
    name: 'batch-large',
    batch: { maxSize: 100, maxInterval: 10000 },
    fn: (tasks) => __awaiter(void 0, void 0, void 0, function* () {
        const batchId = crypto.randomUUID();
        const batchSize = Object.keys(tasks).length;
        const out = {};
        Object.entries(tasks).forEach(([id, input]) => {
            out[id] = {
                batch_id: batchId,
                received: true,
                batch_size: batchSize,
                data_length: input.data.length,
            };
        });
        return out;
    }),
});
exports.batchSingle = hatchet_client_1.hatchet.batchTask({
    name: 'batch-single',
    batch: { maxSize: 1, maxInterval: 100 },
    fn: (tasks) => __awaiter(void 0, void 0, void 0, function* () {
        const batchSize = Object.keys(tasks).length;
        const out = {};
        Object.entries(tasks).forEach(([id, input]) => {
            out[id] = { original: input.message, batch_size: batchSize };
        });
        return out;
    }),
});
exports.batchOrdered = hatchet_client_1.hatchet.batchTask({
    name: 'batch-ordered',
    batch: { maxSize: 20, maxInterval: 2000 },
    fn: (tasks) => __awaiter(void 0, void 0, void 0, function* () {
        const out = {};
        Object.entries(tasks).forEach(([id, input]) => {
            out[id] = { index: input.index };
        });
        return out;
    }),
});
// > Declaring a broadcast batch task
exports.batchBroadcast = hatchet_client_1.hatchet.batchTask({
    name: 'batch-broadcast',
    batch: { maxSize: 10, maxInterval: 2000, broadcastOutput: true },
    fn: (tasks) => __awaiter(void 0, void 0, void 0, function* () {
        const sum = Object.values(tasks).reduce((acc, i) => acc + i.message.length, 0);
        return { sum };
    }),
});
// !!
exports.batchCancel = hatchet_client_1.hatchet.batchTask({
    name: 'batch-cancel',
    batch: { maxSize: 10, maxInterval: 2000, broadcastOutput: true },
    fn: (_tasks, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        yield ctx.cancel();
        return {};
    }),
});
exports.child = hatchet_client_1.hatchet.task({
    name: 'batch-child',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () { return ({ message_len: input.message.length }); }),
});
exports.childBatch = hatchet_client_1.hatchet.batchTask({
    name: 'batch-child-batch',
    batch: { maxSize: 10, maxInterval: 60000, broadcastOutput: true },
    fn: (tasks) => __awaiter(void 0, void 0, void 0, function* () { return ({ out: tasks }); }),
});
exports.batchChildSpawn = hatchet_client_1.hatchet.batchTask({
    name: 'batch-child-spawn',
    batch: { maxSize: 10, maxInterval: 60000 },
    executionTimeout: '60s',
    fn: (tasks) => __awaiter(void 0, void 0, void 0, function* () {
        const out = {};
        yield Promise.all(Object.keys(tasks).map((id) => __awaiter(void 0, void 0, void 0, function* () {
            out[id] = yield exports.child.run({ message: 'blahblah' });
        })));
        return out;
    }),
});
exports.batchChildBatchSpawn = hatchet_client_1.hatchet.batchTask({
    name: 'batch-child-batch-spawn',
    batch: { maxSize: 10, maxInterval: 60000 },
    executionTimeout: '60s',
    fn: (tasks) => __awaiter(void 0, void 0, void 0, function* () {
        const out = {};
        yield Promise.all(Object.keys(tasks).map((id) => __awaiter(void 0, void 0, void 0, function* () {
            out[id] = yield exports.childBatch.run({ message: 'hello' });
        })));
        return out;
    }),
});
