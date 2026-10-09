"use strict";
/* eslint-disable */
/* tslint:disable */
// @ts-nocheck
/*
 * ---------------------------------------------------------------
 * ## THIS FILE WAS GENERATED VIA SWAGGER-TYPESCRIPT-API        ##
 * ##                                                           ##
 * ## AUTHOR: acacode                                           ##
 * ## SOURCE: https://github.com/acacode/swagger-typescript-api ##
 * ---------------------------------------------------------------
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.V1TaskStatus = exports.V1WorkflowType = exports.V1TaskEventType = exports.V1LogLineLevel = exports.V1LogLineOrderByDirection = exports.V1AdditionalMetadataOperator = exports.V1RunningFilter = exports.V1DurableEventLogKind = exports.V1DurableWaitConditionKind = exports.OtelSpanKind = exports.OtelStatusCode = exports.TenantVersion = exports.TenantEnvironment = exports.V1WebhookSourceName = exports.V1WebhookAuthType = exports.V1WebhookHMACAlgorithm = exports.V1WebhookHMACEncoding = exports.V1CELDebugResponseStatus = exports.TenantResource = exports.TenantMemberRole = exports.WorkflowRunStatus = exports.EventOrderByField = exports.EventOrderByDirection = exports.RateLimitOrderByField = exports.RateLimitOrderByDirection = exports.WorkflowPauseScheduledCronRunQueueBehavior = exports.ScheduledWorkflowsMethod = exports.ScheduledWorkflowsOrderByField = exports.WorkflowRunOrderByDirection = exports.ScheduledRunStatus = exports.CronWorkflowsMethod = exports.CronWorkflowsOrderByField = exports.ConcurrencyLimitStrategy = exports.ConcurrencyScope = exports.StepRunStatus = exports.JobRunStatus = exports.StepRunEventReason = exports.StepRunEventSeverity = exports.WorkflowKind = exports.WorkflowRunOrderByField = exports.WorkerStatus = exports.WorkerType = exports.WorkerRuntimeSDKs = exports.WebhookWorkerRequestMethod = exports.FeatureFlagId = exports.PullRequestState = exports.LogLineLevel = exports.LogLineOrderByField = exports.LogLineOrderByDirection = exports.V1TaskRunStatus = void 0;
var V1TaskRunStatus;
(function (V1TaskRunStatus) {
    V1TaskRunStatus["PENDING"] = "PENDING";
    V1TaskRunStatus["RUNNING"] = "RUNNING";
    V1TaskRunStatus["COMPLETED"] = "COMPLETED";
    V1TaskRunStatus["FAILED"] = "FAILED";
    V1TaskRunStatus["CANCELLED"] = "CANCELLED";
})(V1TaskRunStatus || (exports.V1TaskRunStatus = V1TaskRunStatus = {}));
var LogLineOrderByDirection;
(function (LogLineOrderByDirection) {
    LogLineOrderByDirection["Asc"] = "asc";
    LogLineOrderByDirection["Desc"] = "desc";
})(LogLineOrderByDirection || (exports.LogLineOrderByDirection = LogLineOrderByDirection = {}));
var LogLineOrderByField;
(function (LogLineOrderByField) {
    LogLineOrderByField["CreatedAt"] = "createdAt";
})(LogLineOrderByField || (exports.LogLineOrderByField = LogLineOrderByField = {}));
var LogLineLevel;
(function (LogLineLevel) {
    LogLineLevel["DEBUG"] = "DEBUG";
    LogLineLevel["INFO"] = "INFO";
    LogLineLevel["WARN"] = "WARN";
    LogLineLevel["ERROR"] = "ERROR";
})(LogLineLevel || (exports.LogLineLevel = LogLineLevel = {}));
var PullRequestState;
(function (PullRequestState) {
    PullRequestState["Open"] = "open";
    PullRequestState["Closed"] = "closed";
})(PullRequestState || (exports.PullRequestState = PullRequestState = {}));
var FeatureFlagId;
(function (FeatureFlagId) {
    FeatureFlagId["TenantLogWorkflowFilterEnabled"] = "tenant-log-workflow-filter-enabled";
    FeatureFlagId["TraceMinimapEnabled"] = "trace-minimap-enabled";
    FeatureFlagId["OrganizationSsoEnabled"] = "organization-sso-enabled";
    FeatureFlagId["OperatorDetailsEnabled"] = "operator-details-enabled";
})(FeatureFlagId || (exports.FeatureFlagId = FeatureFlagId = {}));
var WebhookWorkerRequestMethod;
(function (WebhookWorkerRequestMethod) {
    WebhookWorkerRequestMethod["GET"] = "GET";
    WebhookWorkerRequestMethod["POST"] = "POST";
    WebhookWorkerRequestMethod["PUT"] = "PUT";
})(WebhookWorkerRequestMethod || (exports.WebhookWorkerRequestMethod = WebhookWorkerRequestMethod = {}));
var WorkerRuntimeSDKs;
(function (WorkerRuntimeSDKs) {
    WorkerRuntimeSDKs["GOLANG"] = "GOLANG";
    WorkerRuntimeSDKs["PYTHON"] = "PYTHON";
    WorkerRuntimeSDKs["TYPESCRIPT"] = "TYPESCRIPT";
    WorkerRuntimeSDKs["RUBY"] = "RUBY";
})(WorkerRuntimeSDKs || (exports.WorkerRuntimeSDKs = WorkerRuntimeSDKs = {}));
var WorkerType;
(function (WorkerType) {
    WorkerType["SELFHOSTED"] = "SELFHOSTED";
    WorkerType["MANAGED"] = "MANAGED";
    WorkerType["WEBHOOK"] = "WEBHOOK";
})(WorkerType || (exports.WorkerType = WorkerType = {}));
var WorkerStatus;
(function (WorkerStatus) {
    WorkerStatus["ACTIVE"] = "ACTIVE";
    WorkerStatus["INACTIVE"] = "INACTIVE";
    WorkerStatus["PAUSED"] = "PAUSED";
})(WorkerStatus || (exports.WorkerStatus = WorkerStatus = {}));
var WorkflowRunOrderByField;
(function (WorkflowRunOrderByField) {
    WorkflowRunOrderByField["CreatedAt"] = "createdAt";
    WorkflowRunOrderByField["StartedAt"] = "startedAt";
    WorkflowRunOrderByField["FinishedAt"] = "finishedAt";
    WorkflowRunOrderByField["Duration"] = "duration";
})(WorkflowRunOrderByField || (exports.WorkflowRunOrderByField = WorkflowRunOrderByField = {}));
var WorkflowKind;
(function (WorkflowKind) {
    WorkflowKind["FUNCTION"] = "FUNCTION";
    WorkflowKind["DURABLE"] = "DURABLE";
    WorkflowKind["DAG"] = "DAG";
})(WorkflowKind || (exports.WorkflowKind = WorkflowKind = {}));
var StepRunEventSeverity;
(function (StepRunEventSeverity) {
    StepRunEventSeverity["INFO"] = "INFO";
    StepRunEventSeverity["WARNING"] = "WARNING";
    StepRunEventSeverity["CRITICAL"] = "CRITICAL";
})(StepRunEventSeverity || (exports.StepRunEventSeverity = StepRunEventSeverity = {}));
var StepRunEventReason;
(function (StepRunEventReason) {
    StepRunEventReason["REQUEUED_NO_WORKER"] = "REQUEUED_NO_WORKER";
    StepRunEventReason["REQUEUED_RATE_LIMIT"] = "REQUEUED_RATE_LIMIT";
    StepRunEventReason["SCHEDULING_TIMED_OUT"] = "SCHEDULING_TIMED_OUT";
    StepRunEventReason["ASSIGNED"] = "ASSIGNED";
    StepRunEventReason["STARTED"] = "STARTED";
    StepRunEventReason["ACKNOWLEDGED"] = "ACKNOWLEDGED";
    StepRunEventReason["FINISHED"] = "FINISHED";
    StepRunEventReason["FAILED"] = "FAILED";
    StepRunEventReason["RETRYING"] = "RETRYING";
    StepRunEventReason["CANCELLED"] = "CANCELLED";
    StepRunEventReason["TIMEOUT_REFRESHED"] = "TIMEOUT_REFRESHED";
    StepRunEventReason["REASSIGNED"] = "REASSIGNED";
    StepRunEventReason["TIMED_OUT"] = "TIMED_OUT";
    StepRunEventReason["SLOT_RELEASED"] = "SLOT_RELEASED";
    StepRunEventReason["RETRIED_BY_USER"] = "RETRIED_BY_USER";
    StepRunEventReason["WORKFLOW_RUN_GROUP_KEY_SUCCEEDED"] = "WORKFLOW_RUN_GROUP_KEY_SUCCEEDED";
    StepRunEventReason["WORKFLOW_RUN_GROUP_KEY_FAILED"] = "WORKFLOW_RUN_GROUP_KEY_FAILED";
})(StepRunEventReason || (exports.StepRunEventReason = StepRunEventReason = {}));
var JobRunStatus;
(function (JobRunStatus) {
    JobRunStatus["PENDING"] = "PENDING";
    JobRunStatus["RUNNING"] = "RUNNING";
    JobRunStatus["SUCCEEDED"] = "SUCCEEDED";
    JobRunStatus["FAILED"] = "FAILED";
    JobRunStatus["CANCELLED"] = "CANCELLED";
    JobRunStatus["BACKOFF"] = "BACKOFF";
})(JobRunStatus || (exports.JobRunStatus = JobRunStatus = {}));
var StepRunStatus;
(function (StepRunStatus) {
    StepRunStatus["PENDING"] = "PENDING";
    StepRunStatus["PENDING_ASSIGNMENT"] = "PENDING_ASSIGNMENT";
    StepRunStatus["ASSIGNED"] = "ASSIGNED";
    StepRunStatus["RUNNING"] = "RUNNING";
    StepRunStatus["SUCCEEDED"] = "SUCCEEDED";
    StepRunStatus["FAILED"] = "FAILED";
    StepRunStatus["CANCELLED"] = "CANCELLED";
    StepRunStatus["CANCELLING"] = "CANCELLING";
    StepRunStatus["BACKOFF"] = "BACKOFF";
})(StepRunStatus || (exports.StepRunStatus = StepRunStatus = {}));
var ConcurrencyScope;
(function (ConcurrencyScope) {
    ConcurrencyScope["WORKFLOW"] = "WORKFLOW";
    ConcurrencyScope["TASK"] = "TASK";
})(ConcurrencyScope || (exports.ConcurrencyScope = ConcurrencyScope = {}));
var ConcurrencyLimitStrategy;
(function (ConcurrencyLimitStrategy) {
    ConcurrencyLimitStrategy["CANCEL_IN_PROGRESS"] = "CANCEL_IN_PROGRESS";
    ConcurrencyLimitStrategy["DROP_NEWEST"] = "DROP_NEWEST";
    ConcurrencyLimitStrategy["QUEUE_NEWEST"] = "QUEUE_NEWEST";
    ConcurrencyLimitStrategy["GROUP_ROUND_ROBIN"] = "GROUP_ROUND_ROBIN";
})(ConcurrencyLimitStrategy || (exports.ConcurrencyLimitStrategy = ConcurrencyLimitStrategy = {}));
var CronWorkflowsOrderByField;
(function (CronWorkflowsOrderByField) {
    CronWorkflowsOrderByField["Name"] = "name";
    CronWorkflowsOrderByField["CreatedAt"] = "createdAt";
})(CronWorkflowsOrderByField || (exports.CronWorkflowsOrderByField = CronWorkflowsOrderByField = {}));
var CronWorkflowsMethod;
(function (CronWorkflowsMethod) {
    CronWorkflowsMethod["DEFAULT"] = "DEFAULT";
    CronWorkflowsMethod["API"] = "API";
})(CronWorkflowsMethod || (exports.CronWorkflowsMethod = CronWorkflowsMethod = {}));
var ScheduledRunStatus;
(function (ScheduledRunStatus) {
    ScheduledRunStatus["PENDING"] = "PENDING";
    ScheduledRunStatus["RUNNING"] = "RUNNING";
    ScheduledRunStatus["SUCCEEDED"] = "SUCCEEDED";
    ScheduledRunStatus["FAILED"] = "FAILED";
    ScheduledRunStatus["CANCELLED"] = "CANCELLED";
    ScheduledRunStatus["QUEUED"] = "QUEUED";
    ScheduledRunStatus["SCHEDULED"] = "SCHEDULED";
})(ScheduledRunStatus || (exports.ScheduledRunStatus = ScheduledRunStatus = {}));
var WorkflowRunOrderByDirection;
(function (WorkflowRunOrderByDirection) {
    WorkflowRunOrderByDirection["ASC"] = "ASC";
    WorkflowRunOrderByDirection["DESC"] = "DESC";
})(WorkflowRunOrderByDirection || (exports.WorkflowRunOrderByDirection = WorkflowRunOrderByDirection = {}));
var ScheduledWorkflowsOrderByField;
(function (ScheduledWorkflowsOrderByField) {
    ScheduledWorkflowsOrderByField["TriggerAt"] = "triggerAt";
    ScheduledWorkflowsOrderByField["CreatedAt"] = "createdAt";
})(ScheduledWorkflowsOrderByField || (exports.ScheduledWorkflowsOrderByField = ScheduledWorkflowsOrderByField = {}));
var ScheduledWorkflowsMethod;
(function (ScheduledWorkflowsMethod) {
    ScheduledWorkflowsMethod["DEFAULT"] = "DEFAULT";
    ScheduledWorkflowsMethod["API"] = "API";
})(ScheduledWorkflowsMethod || (exports.ScheduledWorkflowsMethod = ScheduledWorkflowsMethod = {}));
var WorkflowPauseScheduledCronRunQueueBehavior;
(function (WorkflowPauseScheduledCronRunQueueBehavior) {
    WorkflowPauseScheduledCronRunQueueBehavior["DROP"] = "DROP";
    WorkflowPauseScheduledCronRunQueueBehavior["QUEUE"] = "QUEUE";
})(WorkflowPauseScheduledCronRunQueueBehavior || (exports.WorkflowPauseScheduledCronRunQueueBehavior = WorkflowPauseScheduledCronRunQueueBehavior = {}));
var RateLimitOrderByDirection;
(function (RateLimitOrderByDirection) {
    RateLimitOrderByDirection["Asc"] = "asc";
    RateLimitOrderByDirection["Desc"] = "desc";
})(RateLimitOrderByDirection || (exports.RateLimitOrderByDirection = RateLimitOrderByDirection = {}));
var RateLimitOrderByField;
(function (RateLimitOrderByField) {
    RateLimitOrderByField["Key"] = "key";
    RateLimitOrderByField["Value"] = "value";
    RateLimitOrderByField["LimitValue"] = "limitValue";
})(RateLimitOrderByField || (exports.RateLimitOrderByField = RateLimitOrderByField = {}));
var EventOrderByDirection;
(function (EventOrderByDirection) {
    EventOrderByDirection["Asc"] = "asc";
    EventOrderByDirection["Desc"] = "desc";
})(EventOrderByDirection || (exports.EventOrderByDirection = EventOrderByDirection = {}));
var EventOrderByField;
(function (EventOrderByField) {
    EventOrderByField["CreatedAt"] = "createdAt";
})(EventOrderByField || (exports.EventOrderByField = EventOrderByField = {}));
var WorkflowRunStatus;
(function (WorkflowRunStatus) {
    WorkflowRunStatus["PENDING"] = "PENDING";
    WorkflowRunStatus["RUNNING"] = "RUNNING";
    WorkflowRunStatus["SUCCEEDED"] = "SUCCEEDED";
    WorkflowRunStatus["FAILED"] = "FAILED";
    WorkflowRunStatus["CANCELLED"] = "CANCELLED";
    WorkflowRunStatus["QUEUED"] = "QUEUED";
    WorkflowRunStatus["BACKOFF"] = "BACKOFF";
})(WorkflowRunStatus || (exports.WorkflowRunStatus = WorkflowRunStatus = {}));
var TenantMemberRole;
(function (TenantMemberRole) {
    TenantMemberRole["OWNER"] = "OWNER";
    TenantMemberRole["ADMIN"] = "ADMIN";
    TenantMemberRole["MEMBER"] = "MEMBER";
    TenantMemberRole["VIEWER"] = "VIEWER";
})(TenantMemberRole || (exports.TenantMemberRole = TenantMemberRole = {}));
var TenantResource;
(function (TenantResource) {
    TenantResource["WORKER"] = "WORKER";
    TenantResource["WORKER_SLOT"] = "WORKER_SLOT";
    TenantResource["EVENT"] = "EVENT";
    TenantResource["TASK_RUN"] = "TASK_RUN";
    TenantResource["CRON"] = "CRON";
    TenantResource["SCHEDULE"] = "SCHEDULE";
    TenantResource["INCOMING_WEBHOOK"] = "INCOMING_WEBHOOK";
})(TenantResource || (exports.TenantResource = TenantResource = {}));
/** The status of the CEL evaluation */
var V1CELDebugResponseStatus;
(function (V1CELDebugResponseStatus) {
    V1CELDebugResponseStatus["SUCCESS"] = "SUCCESS";
    V1CELDebugResponseStatus["ERROR"] = "ERROR";
})(V1CELDebugResponseStatus || (exports.V1CELDebugResponseStatus = V1CELDebugResponseStatus = {}));
var V1WebhookHMACEncoding;
(function (V1WebhookHMACEncoding) {
    V1WebhookHMACEncoding["HEX"] = "HEX";
    V1WebhookHMACEncoding["BASE64"] = "BASE64";
    V1WebhookHMACEncoding["BASE64URL"] = "BASE64URL";
})(V1WebhookHMACEncoding || (exports.V1WebhookHMACEncoding = V1WebhookHMACEncoding = {}));
var V1WebhookHMACAlgorithm;
(function (V1WebhookHMACAlgorithm) {
    V1WebhookHMACAlgorithm["SHA1"] = "SHA1";
    V1WebhookHMACAlgorithm["SHA256"] = "SHA256";
    V1WebhookHMACAlgorithm["SHA512"] = "SHA512";
    V1WebhookHMACAlgorithm["MD5"] = "MD5";
})(V1WebhookHMACAlgorithm || (exports.V1WebhookHMACAlgorithm = V1WebhookHMACAlgorithm = {}));
var V1WebhookAuthType;
(function (V1WebhookAuthType) {
    V1WebhookAuthType["BASIC"] = "BASIC";
    V1WebhookAuthType["API_KEY"] = "API_KEY";
    V1WebhookAuthType["HMAC"] = "HMAC";
})(V1WebhookAuthType || (exports.V1WebhookAuthType = V1WebhookAuthType = {}));
var V1WebhookSourceName;
(function (V1WebhookSourceName) {
    V1WebhookSourceName["GENERIC"] = "GENERIC";
    V1WebhookSourceName["GITHUB"] = "GITHUB";
    V1WebhookSourceName["STRIPE"] = "STRIPE";
    V1WebhookSourceName["SLACK"] = "SLACK";
    V1WebhookSourceName["LINEAR"] = "LINEAR";
    V1WebhookSourceName["SVIX"] = "SVIX";
})(V1WebhookSourceName || (exports.V1WebhookSourceName = V1WebhookSourceName = {}));
var TenantEnvironment;
(function (TenantEnvironment) {
    TenantEnvironment["Local"] = "local";
    TenantEnvironment["Development"] = "development";
    TenantEnvironment["Production"] = "production";
})(TenantEnvironment || (exports.TenantEnvironment = TenantEnvironment = {}));
var TenantVersion;
(function (TenantVersion) {
    TenantVersion["V0"] = "V0";
    TenantVersion["V1"] = "V1";
})(TenantVersion || (exports.TenantVersion = TenantVersion = {}));
var OtelStatusCode;
(function (OtelStatusCode) {
    OtelStatusCode["UNSET"] = "UNSET";
    OtelStatusCode["OK"] = "OK";
    OtelStatusCode["ERROR"] = "ERROR";
})(OtelStatusCode || (exports.OtelStatusCode = OtelStatusCode = {}));
var OtelSpanKind;
(function (OtelSpanKind) {
    OtelSpanKind["UNSPECIFIED"] = "UNSPECIFIED";
    OtelSpanKind["INTERNAL"] = "INTERNAL";
    OtelSpanKind["SERVER"] = "SERVER";
    OtelSpanKind["CLIENT"] = "CLIENT";
    OtelSpanKind["PRODUCER"] = "PRODUCER";
    OtelSpanKind["CONSUMER"] = "CONSUMER";
})(OtelSpanKind || (exports.OtelSpanKind = OtelSpanKind = {}));
var V1DurableWaitConditionKind;
(function (V1DurableWaitConditionKind) {
    V1DurableWaitConditionKind["SLEEP"] = "SLEEP";
    V1DurableWaitConditionKind["USER_EVENT"] = "USER_EVENT";
    V1DurableWaitConditionKind["CHILD_WORKFLOW"] = "CHILD_WORKFLOW";
})(V1DurableWaitConditionKind || (exports.V1DurableWaitConditionKind = V1DurableWaitConditionKind = {}));
var V1DurableEventLogKind;
(function (V1DurableEventLogKind) {
    V1DurableEventLogKind["RUN"] = "RUN";
    V1DurableEventLogKind["WAIT_FOR"] = "WAIT_FOR";
    V1DurableEventLogKind["MEMO"] = "MEMO";
})(V1DurableEventLogKind || (exports.V1DurableEventLogKind = V1DurableEventLogKind = {}));
var V1RunningFilter;
(function (V1RunningFilter) {
    V1RunningFilter["ALL"] = "ALL";
    V1RunningFilter["EVICTED"] = "EVICTED";
    V1RunningFilter["ON_WORKER"] = "ON_WORKER";
})(V1RunningFilter || (exports.V1RunningFilter = V1RunningFilter = {}));
var V1AdditionalMetadataOperator;
(function (V1AdditionalMetadataOperator) {
    V1AdditionalMetadataOperator["OR"] = "OR";
    V1AdditionalMetadataOperator["AND"] = "AND";
})(V1AdditionalMetadataOperator || (exports.V1AdditionalMetadataOperator = V1AdditionalMetadataOperator = {}));
var V1LogLineOrderByDirection;
(function (V1LogLineOrderByDirection) {
    V1LogLineOrderByDirection["ASC"] = "ASC";
    V1LogLineOrderByDirection["DESC"] = "DESC";
})(V1LogLineOrderByDirection || (exports.V1LogLineOrderByDirection = V1LogLineOrderByDirection = {}));
var V1LogLineLevel;
(function (V1LogLineLevel) {
    V1LogLineLevel["DEBUG"] = "DEBUG";
    V1LogLineLevel["INFO"] = "INFO";
    V1LogLineLevel["WARN"] = "WARN";
    V1LogLineLevel["ERROR"] = "ERROR";
})(V1LogLineLevel || (exports.V1LogLineLevel = V1LogLineLevel = {}));
var V1TaskEventType;
(function (V1TaskEventType) {
    V1TaskEventType["REQUEUED_NO_WORKER"] = "REQUEUED_NO_WORKER";
    V1TaskEventType["REQUEUED_RATE_LIMIT"] = "REQUEUED_RATE_LIMIT";
    V1TaskEventType["SCHEDULING_TIMED_OUT"] = "SCHEDULING_TIMED_OUT";
    V1TaskEventType["ASSIGNED"] = "ASSIGNED";
    V1TaskEventType["STARTED"] = "STARTED";
    V1TaskEventType["FINISHED"] = "FINISHED";
    V1TaskEventType["FAILED"] = "FAILED";
    V1TaskEventType["RETRYING"] = "RETRYING";
    V1TaskEventType["CANCELLED"] = "CANCELLED";
    V1TaskEventType["TIMED_OUT"] = "TIMED_OUT";
    V1TaskEventType["REASSIGNED"] = "REASSIGNED";
    V1TaskEventType["SLOT_RELEASED"] = "SLOT_RELEASED";
    V1TaskEventType["TIMEOUT_REFRESHED"] = "TIMEOUT_REFRESHED";
    V1TaskEventType["RETRIED_BY_USER"] = "RETRIED_BY_USER";
    V1TaskEventType["SENT_TO_WORKER"] = "SENT_TO_WORKER";
    V1TaskEventType["RATE_LIMIT_ERROR"] = "RATE_LIMIT_ERROR";
    V1TaskEventType["ACKNOWLEDGED"] = "ACKNOWLEDGED";
    V1TaskEventType["CREATED"] = "CREATED";
    V1TaskEventType["QUEUED"] = "QUEUED";
    V1TaskEventType["SKIPPED"] = "SKIPPED";
    V1TaskEventType["WAITING_FOR_BATCH"] = "WAITING_FOR_BATCH";
    V1TaskEventType["BATCH_FLUSHED"] = "BATCH_FLUSHED";
    V1TaskEventType["COULD_NOT_SEND_TO_WORKER"] = "COULD_NOT_SEND_TO_WORKER";
    V1TaskEventType["DURABLE_EVICTED"] = "DURABLE_EVICTED";
    V1TaskEventType["DURABLE_RESTORING"] = "DURABLE_RESTORING";
})(V1TaskEventType || (exports.V1TaskEventType = V1TaskEventType = {}));
var V1WorkflowType;
(function (V1WorkflowType) {
    V1WorkflowType["DAG"] = "DAG";
    V1WorkflowType["TASK"] = "TASK";
})(V1WorkflowType || (exports.V1WorkflowType = V1WorkflowType = {}));
var V1TaskStatus;
(function (V1TaskStatus) {
    V1TaskStatus["QUEUED"] = "QUEUED";
    V1TaskStatus["RUNNING"] = "RUNNING";
    V1TaskStatus["COMPLETED"] = "COMPLETED";
    V1TaskStatus["CANCELLED"] = "CANCELLED";
    V1TaskStatus["FAILED"] = "FAILED";
})(V1TaskStatus || (exports.V1TaskStatus = V1TaskStatus = {}));
