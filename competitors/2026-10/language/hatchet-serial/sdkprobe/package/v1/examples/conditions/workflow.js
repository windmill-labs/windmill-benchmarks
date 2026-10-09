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
exports.dagWithConditions = void 0;
const sleep_1 = __importDefault(require("../../../util/sleep"));
const conditions_1 = require("../../conditions");
const hatchet_client_1 = require("../hatchet-client");
exports.dagWithConditions = hatchet_client_1.hatchet.workflow({
    name: 'simple',
});
const firstTask = exports.dagWithConditions.task({
    name: 'first-task',
    fn: () => __awaiter(void 0, void 0, void 0, function* () {
        yield (0, sleep_1.default)(2000);
        return {
            Completed: true,
        };
    }),
});
exports.dagWithConditions.task({
    name: 'second-task',
    parents: [firstTask],
    waitFor: (0, conditions_1.Or)({ eventKey: 'user:event' }, { sleepFor: '10s' }),
    fn: (_, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        ctx.logger.info('triggered by condition', { triggers: ctx.triggers() });
        return {
            Completed: true,
        };
    }),
});
