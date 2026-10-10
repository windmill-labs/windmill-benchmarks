import { FailureSeverity } from '../../util/failure-severity';
export declare const MAX_TRANSIENT_LISTENER_RETRIES = 3;
export declare function classifyListenerFailure(e: unknown, retries: number): FailureSeverity;
