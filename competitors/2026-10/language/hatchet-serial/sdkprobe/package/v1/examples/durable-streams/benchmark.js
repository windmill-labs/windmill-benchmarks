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
var __asyncValues = (this && this.__asyncValues) || function (o) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var m = o[Symbol.asyncIterator], i;
    return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i);
    function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }
    function settle(resolve, reject, d, v) { Promise.resolve(v).then(function(v) { resolve({ value: v, done: d }); }, reject); }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.describeBenchmark = exports.maxInlinePayloadBytes = exports.MiB = void 0;
exports.latencyStats = latencyStats;
exports.formatLatency = formatLatency;
exports.formatSize = formatSize;
exports.measurePublishLatency = measurePublishLatency;
exports.formatPublishLatency = formatPublishLatency;
exports.openLiveSubscription = openLiveSubscription;
const perf_hooks_1 = require("perf_hooks");
exports.MiB = 1024 * 1024;
// the server's per-message gRPC limit (MaxStreamMessagePayloadBytes); larger payloads are uploaded
exports.maxInlinePayloadBytes = 4 * exports.MiB - 5 * 1024;
// benchmarks run only when asked for (test_durable_streams.sh --bench)
exports.describeBenchmark = process.env.HATCHET_E2E_BENCHMARKS ? describe : describe.skip;
function percentile(sorted, p) {
    return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))];
}
function latencyStats(durationsMs) {
    const sorted = [...durationsMs].sort((a, b) => a - b);
    return {
        count: durationsMs.length,
        avgMs: durationsMs.reduce((sum, d) => sum + d, 0) / durationsMs.length,
        p50Ms: percentile(sorted, 0.5),
        p99Ms: percentile(sorted, 0.99),
        maxMs: sorted[sorted.length - 1],
    };
}
function formatLatency(stats, digits = 2) {
    const fmt = (ms) => ms.toFixed(digits);
    return `avg=${fmt(stats.avgMs)}ms p50=${fmt(stats.p50Ms)}ms p99=${fmt(stats.p99Ms)}ms max=${fmt(stats.maxMs)}ms`;
}
function formatSize(bytes) {
    if (bytes % exports.MiB === 0)
        return `${bytes / exports.MiB}MiB`;
    if (bytes % 1024 === 0)
        return `${bytes / 1024}KiB`;
    return `${bytes}B`;
}
/**
 * Times `count` sequential publishes, after `warmupCount` unmeasured ones
 * absorb connection setup and topic creation.
 */
function measurePublishLatency(hatchet_1, topic_1, payload_1, count_1) {
    return __awaiter(this, arguments, void 0, function* (hatchet, topic, payload, count, warmupCount = 5) {
        for (let i = 0; i < warmupCount; i += 1) {
            yield hatchet.streams.publish(topic, payload);
        }
        const durationsMs = [];
        const start = perf_hooks_1.performance.now();
        for (let i = 0; i < count; i += 1) {
            const before = perf_hooks_1.performance.now();
            yield hatchet.streams.publish(topic, payload);
            durationsMs.push(perf_hooks_1.performance.now() - before);
        }
        const totalMs = perf_hooks_1.performance.now() - start;
        return Object.assign(Object.assign({}, latencyStats(durationsMs)), { throughputPerSec: (count / totalMs) * 1000 });
    });
}
function formatPublishLatency(stats) {
    return `${formatLatency(stats)} throughput=${stats.throughputPerSec.toFixed(2)}/s`;
}
/**
 * Subscribes to topic and resolves once the subscription is live, so a
 * benchmark's timings don't include subscribe setup.
 */
function openLiveSubscription(hatchet, topic) {
    return __awaiter(this, void 0, void 0, function* () {
        const abort = new AbortController();
        let onEvent;
        const consumer = (() => __awaiter(this, void 0, void 0, function* () {
            var _a, e_1, _b, _c;
            try {
                for (var _d = true, _e = __asyncValues(hatchet.streams.events(topic, { signal: abort.signal })), _f; _f = yield _e.next(), _a = _f.done, !_a; _d = true) {
                    _c = _f.value;
                    _d = false;
                    const ev = _c;
                    onEvent === null || onEvent === void 0 ? void 0 : onEvent(ev);
                }
            }
            catch (e_1_1) { e_1 = { error: e_1_1 }; }
            finally {
                try {
                    if (!_d && !_a && (_b = _e.return)) yield _b.call(_e);
                }
                finally { if (e_1) throw e_1.error; }
            }
        }))();
        const nextEvent = () => new Promise((resolve) => {
            onEvent = resolve;
        });
        // the subscription is live once a first message comes through
        const ready = nextEvent();
        yield hatchet.streams.publish(topic, 'ready');
        yield ready;
        return {
            nextEvent,
            close: () => __awaiter(this, void 0, void 0, function* () {
                abort.abort();
                yield consumer;
            }),
        };
    });
}
