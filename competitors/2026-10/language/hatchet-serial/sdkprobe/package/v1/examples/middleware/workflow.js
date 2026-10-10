"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.taskWithMiddleware = void 0;
const client_1 = require("./client");
exports.taskWithMiddleware = client_1.hatchetWithMiddleware.task({
    name: 'task-with-middleware',
    fn: (input, ctx) => {
        ctx.logger.info('task', { message: input.message }); // string  (from TaskInput)
        ctx.logger.info('task', { first: input.first }); // number  (from GlobalInputType)
        ctx.logger.info('task', { second: input.second }); // number  (from GlobalInputType)
        ctx.logger.info('task', { dependency: input.dependency }); // string  (from Pre Middleware)
        return {
            message: input.message,
            extra: 1,
        };
    },
});
// !!
