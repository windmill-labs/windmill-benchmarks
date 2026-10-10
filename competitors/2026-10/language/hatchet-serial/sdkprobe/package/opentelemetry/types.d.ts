export type { OpenTelemetryConfig } from '../clients/hatchet-client/client-config';
export declare const DEFAULT_CONFIG: {
    excludedAttributes: string[];
    includeTaskNameInSpanName: boolean;
    individualRunSpansForBulkRun: boolean;
    enableHatchetCollector: boolean;
};
