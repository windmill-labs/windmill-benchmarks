"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const hatchet_client_1 = require("../hatchet-client");
const workflow_1 = require("./workflow");
// > Create a filter
hatchet_client_1.hatchet.filters.create({
    workflowId: workflow_1.lower.name,
    expression: 'input.ShouldSkip == false',
    scope: 'foobarbaz',
    payload: {
        main_character: 'Anna',
        supporting_character: 'Stiva',
        location: 'Moscow',
    },
});
// !!
// > Skip a run
hatchet_client_1.hatchet.events.push(workflow_1.SIMPLE_EVENT, {
    Message: 'hello',
    ShouldSkip: true,
}, {
    scope: 'foobarbaz',
});
// !!
// > Trigger a run
hatchet_client_1.hatchet.events.push(workflow_1.SIMPLE_EVENT, {
    Message: 'hello',
    ShouldSkip: false,
}, {
    scope: 'foobarbaz',
});
// !!
