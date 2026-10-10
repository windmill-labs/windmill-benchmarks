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
Object.defineProperty(exports, "__esModule", { value: true });
exports.GET = GET;
const stream_1 = require("stream");
const hatchet_client_1 = require("../hatchet-client");
const workflow_1 = require("./workflow");
// > NextJS Proxy
function GET() {
    return __awaiter(this, void 0, void 0, function* () {
        try {
            const ref = yield workflow_1.streamingTask.runNoWait({});
            const workflowRunId = yield ref.getWorkflowRunId();
            const stream = stream_1.Readable.from(hatchet_client_1.hatchet.runs.subscribeToStream(workflowRunId));
            return new Response(stream_1.Readable.toWeb(stream), {
                headers: {
                    'Content-Type': 'text/plain',
                    'Cache-Control': 'no-cache',
                    Connection: 'keep-alive',
                },
            });
        }
        catch (error) {
            return new Response('Internal Server Error', { status: 500 });
        }
    });
}
// !!
