import { Logger } from '../../../util/logger';
export declare const workerStatus: {
    readonly INITIALIZED: "INITIALIZED";
    readonly STARTING: "STARTING";
    readonly HEALTHY: "HEALTHY";
    readonly UNHEALTHY: "UNHEALTHY";
};
export type WorkerStatus = (typeof workerStatus)[keyof typeof workerStatus];
export declare class HealthServer {
    private port;
    private getStatus;
    private workerName;
    private getSlots;
    private getActions;
    private getLabels;
    private logger;
    private server;
    private register;
    private workerStatusGauge;
    private workerSlotsGauge;
    private workerActionsGauge;
    private metricsInitialized;
    constructor(port: number, getStatus: () => WorkerStatus, workerName: string, getSlots: () => number, getActions: () => string[], getLabels: () => Record<string, string | number>, logger: Logger);
    private handleRequest;
    private handleHealth;
    private handleReadyz;
    private handleLivez;
    private initializeMetrics;
    private handleMetrics;
    start(): Promise<void>;
    stop(): Promise<void>;
}
