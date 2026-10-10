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
const conditions_1 = require("../../conditions");
const hatchet_client_1 = require("../hatchet-client");
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        // > Declaring a Durable Task
        const simple = hatchet_client_1.hatchet.durableTask({
            name: 'simple',
            fn: (input, ctx) => __awaiter(this, void 0, void 0, function* () {
                yield ctx.waitFor((0, conditions_1.Or)({
                    eventKey: 'user:pay',
                    expression: 'input.Status == "PAID"',
                }, {
                    sleepFor: '24h',
                }));
                return {
                    TransformedMessage: input.Message.toLowerCase(),
                };
            }),
        });
        // !!
        // > Running a Task
        const result = yield simple.run({ Message: 'Hello, World!' });
        // !!
    });
}
if (require.main === module) {
    main();
}
