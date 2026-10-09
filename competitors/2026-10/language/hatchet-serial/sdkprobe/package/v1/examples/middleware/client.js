"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.hatchetWithMiddlewareChaining = exports.hatchetWithMiddleware = void 0;
// > Init a client with middleware
const v1_1 = require("../..");
const myMiddleware = {
    before: (input, ctx) => {
        ctx.logger.info('before', { first: input.first });
        return Object.assign(Object.assign({}, input), { dependency: 'abc-123' });
    },
    after: (output, ctx, input) => {
        return Object.assign(Object.assign({}, output), { additionalData: 2 });
    },
};
exports.hatchetWithMiddleware = v1_1.HatchetClient.init().withMiddleware(myMiddleware);
// !!
// > Chaining middleware
const firstMiddleware = {
    before: (input, ctx) => {
        ctx.logger.info('before', { first: input.first });
        return Object.assign(Object.assign({}, input), { dependency: 'abc-123' });
    },
    after: (output, ctx, input) => {
        return Object.assign(Object.assign({}, output), { firstExtra: 3 });
    },
};
const secondMiddleware = {
    before: (input, ctx) => {
        ctx.logger.info('before', { dependency: input.dependency }); // available from previous middleware
        return Object.assign(Object.assign({}, input), { anotherDep: true });
    },
    after: (output, ctx, input) => {
        return Object.assign(Object.assign({}, output), { secondExtra: 4 });
    },
};
exports.hatchetWithMiddlewareChaining = v1_1.HatchetClient.init()
    .withMiddleware(firstMiddleware)
    .withMiddleware(secondMiddleware);
// !!
