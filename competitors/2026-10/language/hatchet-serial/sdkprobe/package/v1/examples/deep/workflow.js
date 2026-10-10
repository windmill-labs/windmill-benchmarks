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
exports.parent = exports.child5 = exports.child4 = exports.child3 = exports.child2 = exports.child1 = void 0;
const sleep_1 = __importDefault(require("../../../util/sleep"));
const hatchet_client_1 = require("../hatchet-client");
exports.child1 = hatchet_client_1.hatchet.workflow({
    name: 'child1',
});
exports.child1.task({
    name: 'transformer',
    fn: () => {
        (0, sleep_1.default)(15);
        return {
            Sum: 1,
        };
    },
});
exports.child2 = hatchet_client_1.hatchet.workflow({
    name: 'child2',
});
exports.child2.task({
    name: 'transformer',
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const count = input.N;
        const promises = Array(count)
            .fill(null)
            .map(() => ({ workflow: exports.child1, input }));
        const results = yield ctx.bulkRunChildren(promises);
        (0, sleep_1.default)(15);
        return {
            Sum: results.reduce((acc, r) => acc + r.transformer.Sum, 0),
        };
    }),
});
exports.child3 = hatchet_client_1.hatchet.workflow({
    name: 'child3',
});
exports.child3.task({
    name: 'transformer',
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const count = input.N;
        const promises = Array(count)
            .fill(null)
            .map(() => ({ workflow: exports.child2, input }));
        const results = yield ctx.bulkRunChildren(promises);
        return {
            Sum: results.reduce((acc, r) => acc + r.transformer.Sum, 0),
        };
    }),
});
exports.child4 = hatchet_client_1.hatchet.workflow({
    name: 'child4',
});
exports.child4.task({
    name: 'transformer',
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const count = input.N;
        const promises = Array(count)
            .fill(null)
            .map(() => ({ workflow: exports.child3, input }));
        const results = yield ctx.bulkRunChildren(promises);
        return {
            Sum: results.reduce((acc, r) => acc + r.transformer.Sum, 0),
        };
    }),
});
exports.child5 = hatchet_client_1.hatchet.workflow({
    name: 'child5',
});
exports.child5.task({
    name: 'transformer',
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const count = input.N;
        const promises = Array(count)
            .fill(null)
            .map(() => ({ workflow: exports.child4, input }));
        const results = yield ctx.bulkRunChildren(promises);
        return {
            Sum: results.reduce((acc, r) => acc + r.transformer.Sum, 0),
        };
    }),
});
exports.parent = hatchet_client_1.hatchet.workflow({
    name: 'parent',
});
exports.parent.task({
    name: 'parent',
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const count = input.N; // Random number between 2-4
        const promises = Array(count)
            .fill(null)
            .map(() => ({ workflow: exports.child5, input }));
        const results = yield ctx.bulkRunChildren(promises);
        return {
            Sum: results.reduce((acc, r) => acc + r.transformer.Sum, 0),
        };
    }),
});
