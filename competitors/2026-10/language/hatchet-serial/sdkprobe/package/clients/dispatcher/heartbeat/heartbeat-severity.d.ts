import { FailureSeverity } from '../../../util/failure-severity';
export declare const MAX_MISSED_HEARTBEATS = 3;
export declare function classifyHeartbeatFailure(e: unknown, missedHeartbeats: number): FailureSeverity;
