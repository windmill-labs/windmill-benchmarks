"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.firstTask = void 0;
const hatchet_client_1 = require("../../hatchet-client");
exports.firstTask = hatchet_client_1.hatchet.task({
    name: 'first-task',
    fn: (input, ctx) => {
        return {
            TransformedMessage: input.Message.toLowerCase(),
        };
    },
});
