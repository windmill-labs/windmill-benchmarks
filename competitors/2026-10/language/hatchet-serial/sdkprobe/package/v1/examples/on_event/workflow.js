"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.upper = exports.lowerWithFilter = exports.lower = exports.SIMPLE_EVENT = void 0;
const hatchet_client_1 = require("../hatchet-client");
exports.SIMPLE_EVENT = 'simple-event:create';
// > Run workflow on event
exports.lower = hatchet_client_1.hatchet.workflow({
    name: 'lower',
    // 👀 Declare the event that will trigger the workflow
    onEvents: ['simple-event:create'],
});
// !!
// > Workflow with filter
exports.lowerWithFilter = hatchet_client_1.hatchet.workflow({
    name: 'lower',
    // 👀 Declare the event that will trigger the workflow
    onEvents: ['simple-event:create'],
    defaultFilters: [
        {
            expression: 'true',
            scope: 'example-scope',
            payload: {
                mainCharacter: 'Anna',
                supportingCharacter: 'Stiva',
                location: 'Moscow',
            },
        },
    ],
});
// !!
exports.lower.task({
    name: 'lower',
    fn: (input) => {
        return {
            TransformedMessage: input.Message.toLowerCase(),
        };
    },
});
exports.upper = hatchet_client_1.hatchet.workflow({
    name: 'upper',
    on: {
        event: exports.SIMPLE_EVENT,
    },
});
exports.upper.task({
    name: 'upper',
    fn: (input) => {
        return {
            TransformedMessage: input.Message.toUpperCase(),
        };
    },
});
// > Accessing the filter payload
exports.lowerWithFilter.task({
    name: 'lowerWithFilter',
    fn: (input, ctx) => {
        ctx.logger.info('filterPayload', { filterPayload: ctx.filterPayload() });
    },
});
// !!
