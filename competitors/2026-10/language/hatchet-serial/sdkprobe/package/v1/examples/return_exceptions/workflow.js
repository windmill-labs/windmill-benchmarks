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
exports.returnExceptionsTask = void 0;
const hatchet_client_1 = require("../hatchet-client");
exports.returnExceptionsTask = hatchet_client_1.hatchet.task({
    name: 'return-exceptions-task',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        if (input.index % 2 === 0) {
            throw new Error(`error in task with index ${input.index}`);
        }
        return { message: 'this is a successful task.' };
    }),
});
