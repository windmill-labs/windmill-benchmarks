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
exports.WorkersClient = void 0;
/**
 * The workers client is a client for managing workers programmatically within Hatchet.
 */
class WorkersClient {
    constructor(client) {
        this.api = client.api;
        this.tenantId = client.tenantId;
    }
    /**
     * Get a worker by its ID.
     * @param workerId - The ID of the worker to get.
     * @returns A promise that resolves to the worker.
     */
    get(workerId) {
        return __awaiter(this, void 0, void 0, function* () {
            const { data } = yield this.api.workerGet(workerId);
            return data;
        });
    }
    /**
     * List all workers in the tenant.
     * @returns A promise that resolves to the list of workers.
     */
    list() {
        return __awaiter(this, void 0, void 0, function* () {
            const { data } = yield this.api.workerList(this.tenantId);
            return data;
        });
    }
    /**
     * Check if a worker is paused.
     * @param workerId - The ID of the worker to check.
     * @returns A promise that resolves to true if the worker is paused, false otherwise.
     */
    isPaused(workerId) {
        return __awaiter(this, void 0, void 0, function* () {
            const wf = yield this.get(workerId);
            return wf.status === 'PAUSED';
        });
    }
    /**
     * Pause a worker.
     * @param workerId - The ID of the worker to pause.
     * @returns A promise that resolves to the paused worker.
     */
    pause(workerId) {
        return __awaiter(this, void 0, void 0, function* () {
            const { data } = yield this.api.workerUpdate(workerId, {
                isPaused: true,
            });
            return data;
        });
    }
    /**
     * Unpause a worker.
     * @param workerId - The ID of the worker to unpause.
     * @returns A promise that resolves to the unpaused worker.
     */
    unpause(workerId) {
        return __awaiter(this, void 0, void 0, function* () {
            const { data } = yield this.api.workerUpdate(workerId, {
                isPaused: false,
            });
            return data;
        });
    }
}
exports.WorkersClient = WorkersClient;
