"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DEFAULT_DURABLE_TASK_EVICTION_POLICY = void 0;
exports.DEFAULT_DURABLE_TASK_EVICTION_POLICY = {
    ttl: '15m',
    allowCapacityEviction: true,
    priority: 0,
};
