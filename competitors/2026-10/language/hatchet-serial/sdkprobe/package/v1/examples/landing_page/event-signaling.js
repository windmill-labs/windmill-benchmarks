"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.simple = void 0;
const hatchet_client_1 = require("../hatchet-client");
// > Trigger on an event
exports.simple = hatchet_client_1.hatchet.task({
    name: 'simple',
    onEvents: ['user:created'],
    fn: (input) => {
        // ...
        return {
            TransformedMessage: input.Message.toLowerCase(),
        };
    },
});
// !!
