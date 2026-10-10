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
exports.imageProcessor = void 0;
exports.processImageTask = processImageTask;
const hatchet_client_1 = require("./hatchet-client");
function processImage(imageUrl, filters) {
    // Do some image processing
    return Promise.resolve({ url: imageUrl, size: 100, format: 'png' });
}
// > Before (Mergent)
function processImageTask(req) {
    return __awaiter(this, void 0, void 0, function* () {
        const { imageUrl, filters } = req.body;
        try {
            const result = yield processImage(imageUrl, filters);
            return { success: true, processedUrl: result.url };
        }
        catch (error) {
            console.error('Image processing failed:', error);
            throw error;
        }
    });
}
exports.imageProcessor = hatchet_client_1.hatchet.task({
    name: 'image-processor',
    retries: 3,
    executionTimeout: '10m',
    fn: (_a) => __awaiter(void 0, [_a], void 0, function* ({ imageUrl, filters }) {
        // Do some image processing
        const result = yield processImage(imageUrl, filters);
        if (!result.url) {
            throw new Error('Processing failed to generate URL');
        }
        return {
            processedUrl: result.url,
            metadata: {
                size: result.size,
                format: result.format,
                appliedFilters: filters,
            },
        };
    }),
});
// !!
function run() {
    return __awaiter(this, void 0, void 0, function* () {
        // > Running a task (Mergent)
        const options = {
            method: 'POST',
            headers: { Authorization: 'Bearer <token>', 'Content-Type': 'application/json' },
            body: JSON.stringify({
                name: '4cf95241-fa19-47ef-8a67-71e483747649',
                queue: 'default',
                request: {
                    url: 'https://example.com',
                    headers: { Authorization: 'fake-secret-token', 'Content-Type': 'application/json' },
                    body: 'Hello, world!',
                },
            }),
        };
        fetch('https://api.mergent.co/v2/tasks', options)
            .then((response) => response.json())
            .then((response) => console.log(response))
            .catch((err) => console.error(err));
        // !!
        // > Running a task (Hatchet)
        const result = yield exports.imageProcessor.run({
            imageUrl: 'https://example.com/image.png',
            filters: ['blur'],
        });
        // you can await fully typed results
        console.log(result);
        // !!
    });
}
function schedule() {
    return __awaiter(this, void 0, void 0, function* () {
        // > Scheduling tasks (Mergent)
        const options = {
            // same options as before
            body: JSON.stringify({
                // same body as before
                delay: '5m',
            }),
        };
        // !!
        // > Scheduling tasks (Hatchet)
        // Schedule the task to run at a specific time
        const runAt = new Date(Date.now() + 1000 * 60 * 60 * 24);
        exports.imageProcessor.schedule(runAt, {
            imageUrl: 'https://example.com/image.png',
            filters: ['blur'],
        });
        // Schedule the task to run every hour
        exports.imageProcessor.cron('run-hourly', '0 * * * *', {
            imageUrl: 'https://example.com/image.png',
            filters: ['blur'],
        });
        // !!
    });
}
