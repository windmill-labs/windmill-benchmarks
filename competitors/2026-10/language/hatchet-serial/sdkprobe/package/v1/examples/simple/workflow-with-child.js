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
exports.parent = exports.child3 = exports.child2 = exports.child1 = exports.child = void 0;
// > Declaring a Task
const sleep_1 = __importDefault(require("../../../util/sleep"));
const hatchet_client_1 = require("../hatchet-client");
exports.child = hatchet_client_1.hatchet.workflow({
    name: 'child',
});
exports.child1 = exports.child.task({
    name: 'child1',
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        yield (0, sleep_1.default)(30 * 1000);
        ctx.logger.info('hello from the child1', { hello: 'moon' });
        return {
            TransformedMessage: input.Message.toLowerCase(),
        };
    }),
});
exports.child2 = exports.child.task({
    name: 'child2',
    fn: (input, ctx) => {
        ctx.logger.info('hello from the child2');
        return {
            TransformedMessage: input.Message.toLowerCase(),
        };
    },
});
exports.child3 = exports.child.task({
    name: 'child3',
    parents: [exports.child1, exports.child2],
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        ctx.logger.info('hello from the child3');
        return {
            TransformedMessage: input.Message.toLowerCase(),
        };
    }),
});
exports.parent = hatchet_client_1.hatchet.task({
    name: 'parent',
    executionTimeout: '5m',
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const c = yield ctx.runChild(exports.child, {
            Message: input.Message,
        });
        return {
            TransformedMessage: 'not implemented',
        };
    }),
});
// !!
// see ./worker.ts and ./run.ts for how to run the workflow
