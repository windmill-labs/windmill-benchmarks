"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.simple = void 0;
const hatchet_client_1 = require("../hatchet-client");
// > Route tasks to workers with matching labels
exports.simple = hatchet_client_1.hatchet.task({
    name: 'simple',
    desiredWorkerLabels: {
        cpu: {
            value: '2x',
        },
    },
    fn: (input) => {
        return {
            TransformedMessage: input.Message.toLowerCase(),
        };
    },
});
hatchet_client_1.hatchet.worker('task-routing-worker', {
    workflows: [exports.simple],
    labels: {
        cpu: process.env.CPU_LABEL,
    },
});
// !!
