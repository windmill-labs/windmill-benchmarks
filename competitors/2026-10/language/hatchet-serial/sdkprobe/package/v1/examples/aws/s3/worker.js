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
var _a, _b, _c, _d;
Object.defineProperty(exports, "__esModule", { value: true });
const client_s3_1 = require("@aws-sdk/client-s3");
const v1_1 = require("../../..");
const hatchet_client_1 = require("../../hatchet-client");
const BUCKET_PREFIX = (_a = process.env.S3_WORKER_BUCKET_PREFIX) !== null && _a !== void 0 ? _a : 'bucket-';
const MAX_CONCURRENT_BUCKET_POLLERS = parseInt((_b = process.env.S3_WORKER_MAX_CONCURRENT_BUCKET_POLLERS) !== null && _b !== void 0 ? _b : '10');
const MAX_RUNS_PER_BUCKET = parseInt((_c = process.env.S3_WORKER_MAX_RUNS_PER_BUCKET) !== null && _c !== void 0 ? _c : '20');
const SLOTS = parseInt((_d = process.env.S3_WORKER_SLOTS) !== null && _d !== void 0 ? _d : '40');
// > Client Setup
const s3 = new client_s3_1.S3Client({ forcePathStyle: true });
// !!
// > Fetch S3 Buckets
const fetchBucketsWorkflow = hatchet_client_1.hatchet.workflow({
    name: 'fetch_s3_buckets',
    on: {
        cron: '* * * * *',
    },
    concurrency: {
        expression: "'singleton'",
        maxRuns: 1,
        limitStrategy: v1_1.ConcurrencyLimitStrategy.CANCEL_NEWEST,
    },
});
// !!
// > Fetch S3 Objects
const fetchObjectsWorkflow = hatchet_client_1.hatchet.workflow({
    name: 'fetch_s3_objects',
    concurrency: [
        {
            expression: 'input.bucket',
            maxRuns: 1,
            limitStrategy: v1_1.ConcurrencyLimitStrategy.CANCEL_NEWEST,
        },
        {
            expression: "'constant'",
            maxRuns: MAX_CONCURRENT_BUCKET_POLLERS,
            limitStrategy: v1_1.ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
        },
    ],
});
// !!
// > Process S3 Objects
const processObjectWorkflow = hatchet_client_1.hatchet.workflow({
    name: 'process_object',
    concurrency: {
        expression: 'input.bucket',
        maxRuns: MAX_RUNS_PER_BUCKET,
        limitStrategy: v1_1.ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
    },
});
// !!
// > Fetch S3 Buckets Task
fetchBucketsWorkflow.task({
    name: 'fetch_buckets',
    fn: () => __awaiter(void 0, void 0, void 0, function* () {
        var _a, e_1, _b, _c;
        var _d;
        try {
            for (var _e = true, _f = __asyncValues((0, client_s3_1.paginateListBuckets)({ client: s3, pageSize: 10 }, { Prefix: BUCKET_PREFIX })), _g; _g = yield _f.next(), _a = _g.done, !_a; _e = true) {
                _c = _g.value;
                _e = false;
                const page = _c;
                const items = ((_d = page.Buckets) !== null && _d !== void 0 ? _d : [])
                    .filter((bucket) => bucket.Name !== undefined)
                    .map((bucket) => ({
                    input: { bucket: bucket.Name },
                    opts: {
                        childKey: bucket.Name,
                        additionalMetadata: { 'bucket-name': bucket.Name },
                    },
                }));
                if (items.length > 0) {
                    yield fetchObjectsWorkflow.runManyNoWait(items);
                }
            }
        }
        catch (e_1_1) { e_1 = { error: e_1_1 }; }
        finally {
            try {
                if (!_e && !_a && (_b = _f.return)) yield _b.call(_f);
            }
            finally { if (e_1) throw e_1.error; }
        }
        return {};
    }),
});
// !!
// > Fetch S3 Objects Task
fetchObjectsWorkflow.task({
    name: 'fetch_objects',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        var _a, e_2, _b, _c;
        var _d;
        try {
            for (var _e = true, _f = __asyncValues((0, client_s3_1.paginateListObjectsV2)({ client: s3, pageSize: 100 }, { Bucket: input.bucket })), _g; _g = yield _f.next(), _a = _g.done, !_a; _e = true) {
                _c = _g.value;
                _e = false;
                const page = _c;
                const items = ((_d = page.Contents) !== null && _d !== void 0 ? _d : [])
                    .filter((obj) => obj.Key !== undefined)
                    .map((obj) => ({
                    input: { bucket: input.bucket, key: obj.Key },
                    opts: {
                        childKey: `${input.bucket}/${obj.Key}`,
                    },
                }));
                if (items.length > 0) {
                    yield processObjectWorkflow.runManyNoWait(items);
                }
            }
        }
        catch (e_2_1) { e_2 = { error: e_2_1 }; }
        finally {
            try {
                if (!_e && !_a && (_b = _f.return)) yield _b.call(_f);
            }
            finally { if (e_2) throw e_2.error; }
        }
        return {};
    }),
});
// !!
// > Download and Process S3 Objects Task
processObjectWorkflow.task({
    name: 'process_object',
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        let body;
        try {
            const response = yield s3.send(new client_s3_1.GetObjectCommand({ Bucket: input.bucket, Key: input.key }));
            if (!response.Body) {
                return {};
            }
            body = Buffer.from(yield response.Body.transformToByteArray());
        }
        catch (err) {
            if (err instanceof client_s3_1.NoSuchKey || err instanceof client_s3_1.NoSuchBucket) {
                yield ctx.log(`skipping ${input.bucket}/${input.key}: not found`);
                return {};
            }
            throw err;
        }
        // TODO: actual image processing here
        yield s3.send(new client_s3_1.DeleteObjectCommand({ Bucket: input.bucket, Key: input.key }));
        return {};
    }),
});
// !!
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        const worker = yield hatchet_client_1.hatchet.worker('s3-worker', {
            workflows: [fetchBucketsWorkflow, fetchObjectsWorkflow, processObjectWorkflow],
            slots: SLOTS,
        });
        yield worker.start();
    });
}
if (require.main === module) {
    main();
}
