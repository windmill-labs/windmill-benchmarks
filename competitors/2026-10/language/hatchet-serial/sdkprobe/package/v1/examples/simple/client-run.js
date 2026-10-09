"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
// > Client Run Methods
const hatchet_client_1 = require("../hatchet-client");
hatchet_client_1.hatchet.run('simple', { Message: 'Hello, World!' });
hatchet_client_1.hatchet.runNoWait('simple', { Message: 'Hello, World!' }, {});
hatchet_client_1.hatchet.schedules.create('simple', {
    triggerAt: new Date(Date.now() + 1000 * 60 * 60 * 24),
    input: { Message: 'Hello, World!' },
});
hatchet_client_1.hatchet.crons.create('simple', {
    name: 'my-cron',
    expression: '0 0 * * *',
    input: { Message: 'Hello, World!' },
});
// !!
