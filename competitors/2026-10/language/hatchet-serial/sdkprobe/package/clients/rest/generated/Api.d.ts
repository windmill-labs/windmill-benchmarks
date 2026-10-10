import { APIErrors, APIMeta, AcceptInviteRequest, BulkCreateEventRequest, CancelEventRequest, CreateAPITokenRequest, CreateAPITokenResponse, CreateCronWorkflowTriggerRequest, CreateEventRequest, CreateSNSIntegrationRequest, CreateTenantAlertEmailGroupRequest, CreateTenantInviteRequest, CreateTenantRequest, CronWorkflows, CronWorkflowsList, CronWorkflowsOrderByField, Event, EventData, EventKey, EventKeyList, EventList, EventOrderByDirection, EventOrderByField, EventSearch, Events, FeatureFlagEvaluationResult, FeatureFlagId, ListAPIMetaIntegration, ListAPITokensResponse, ListSNSIntegrations, ListSlackWebhooks, OtelSpanList, RateLimitList, RateLimitOrderByDirection, RateLimitOrderByField, RejectInviteRequest, ReplayEventRequest, ReplayWorkflowRunsRequest, ReplayWorkflowRunsResponse, RerunStepRunRequest, SNSIntegration, ScheduleWorkflowRunRequest, ScheduledRunStatus, ScheduledWorkflows, ScheduledWorkflowsBulkDeleteRequest, ScheduledWorkflowsBulkDeleteResponse, ScheduledWorkflowsBulkUpdateRequest, ScheduledWorkflowsBulkUpdateResponse, ScheduledWorkflowsList, ScheduledWorkflowsOrderByField, StepRun, StepRunArchiveList, StepRunEventList, TaskStats, Tenant, TenantAlertEmailGroup, TenantAlertEmailGroupList, TenantAlertingSettings, TenantInvite, TenantInviteList, TenantMember, TenantMemberList, TenantQueueMetrics, TenantResourcePolicy, TenantStepRunQueueMetrics, TriggerRunResult, TriggerWorkflowRunRequest, UpdateCronWorkflowTriggerRequest, UpdateScheduledWorkflowRunRequest, UpdateTenantAlertEmailGroupRequest, UpdateTenantInviteRequest, UpdateTenantMemberRequest, UpdateTenantRequest, UpdateWorkerRequest, User, UserChangePasswordRequest, UserLoginRequest, UserRegisterRequest, UserTenantMembershipsList, V1AdditionalMetadataOperator, V1BranchDurableTaskRequest, V1BranchDurableTaskResponse, V1CELDebugRequest, V1CELDebugResponse, V1CancelTaskRequest, V1CancelledTasks, V1CreateFilterRequest, V1CreateWebhookRequest, V1DagChildren, V1DurableEventLogList, V1Event, V1EventList, V1Filter, V1FilterList, V1LogLineLevel, V1LogLineList, V1LogLineOrderByDirection, V1LogsPointMetrics, V1ReplayTaskRequest, V1ReplayedTasks, V1RestoreTaskResponse, V1RunningFilter, V1StreamPayloadRef, V1TaskEventList, V1TaskPointMetrics, V1TaskRunMetrics, V1TaskStatus, V1TaskSummary, V1TaskSummaryList, V1TaskTimingList, V1TriggerWorkflowRunRequest, V1UpdateFilterRequest, V1UpdateWebhookRequest, V1Webhook, V1WebhookList, V1WebhookResponse, V1WebhookSourceName, V1WorkflowRunDetails, V1WorkflowRunDisplayNameList, V1WorkflowRunExternalIdList, WebhookWorkerCreateRequest, WebhookWorkerCreated, WebhookWorkerListResponse, WebhookWorkerRequestListResponse, Worker, WorkerList, WorkerStatus, Workflow, WorkflowID, WorkflowKindList, WorkflowList, WorkflowMetrics, WorkflowRun, WorkflowRunList, WorkflowRunOrderByDirection, WorkflowRunOrderByField, WorkflowRunShape, WorkflowRunStatus, WorkflowRunStatusList, WorkflowRunsCancelRequest, WorkflowRunsMetrics, WorkflowUpdateRequest, WorkflowVersion, WorkflowWorkersCount } from './data-contracts';
import { HttpClient, RequestParams } from './http-client';
export declare class Api<SecurityDataType = unknown> extends HttpClient<SecurityDataType> {
    /**
     * @description Get a task by id
     *
     * @tags Task
     * @name V1TaskGet
     * @summary Get a task
     * @request GET:/api/v1/stable/tasks/{task}
     * @secure
     */
    v1TaskGet: (task: string, query?: {
        /** The attempt number */
        attempt?: number;
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1TaskSummary, any, {}>>;
    /**
     * @description List events for a task
     *
     * @tags Task
     * @name V1TaskEventList
     * @summary List events for a task
     * @request GET:/api/v1/stable/tasks/{task}/task-events
     * @secure
     */
    v1TaskEventList: (task: string, query?: {
        /**
         * The number to skip
         * @format int64
         */
        offset?: number;
        /**
         * The number to limit by
         * @format int64
         */
        limit?: number;
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1TaskEventList, any, {}>>;
    /**
     * @description Lists log lines for a task
     *
     * @tags Log
     * @name V1LogLineList
     * @summary List log lines
     * @request GET:/api/v1/stable/tasks/{task}/logs
     * @secure
     */
    v1LogLineList: (task: string, query?: {
        /**
         * The number to limit by
         * @format int64
         */
        limit?: number;
        /**
         * The start time to get logs for
         * @format date-time
         */
        since?: string;
        /**
         * The end time to get logs for
         * @format date-time
         */
        until?: string;
        /** A full-text search query to filter for */
        search?: string;
        /** The log level(s) to include */
        levels?: V1LogLineLevel[];
        /** The direction to order by */
        order_by_direction?: V1LogLineOrderByDirection;
        /** The attempt number to filter for */
        attempt?: number;
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1LogLineList, any, {}>>;
    /**
     * @description Cancel tasks
     *
     * @tags Task
     * @name V1TaskCancel
     * @summary Cancel tasks
     * @request POST:/api/v1/stable/tenants/{tenant}/tasks/cancel
     * @secure
     */
    v1TaskCancel: (tenant: string, data: V1CancelTaskRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1CancelledTasks, any, {}>>;
    /**
     * @description Lists log lines for a tenant
     *
     * @tags Log
     * @name V1TenantLogLineList
     * @summary List log lines
     * @request GET:/api/v1/stable/tenants/{tenant}/logs
     * @secure
     */
    v1TenantLogLineList: (tenant: string, query?: {
        /**
         * The number to limit by
         * @format int64
         */
        limit?: number;
        /**
         * The start time to get logs for
         * @format date-time
         */
        since?: string;
        /**
         * The end time to get logs for
         * @format date-time
         */
        until?: string;
        /** A full-text search query to filter for */
        search?: string;
        /** The log level(s) to include */
        levels?: V1LogLineLevel[];
        /** The direction to order by */
        order_by_direction?: V1LogLineOrderByDirection;
        /** The attempt number to filter for */
        attempt?: number;
        /** The task external ID(s) to filter by */
        taskExternalIds?: string[];
        /** The workflow id(s) to filter for */
        workflow_ids?: string[];
        /** The step id(s) to filter for */
        step_ids?: string[];
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1LogLineList, any, {}>>;
    /**
     * @description Get a minute by minute breakdown of log metrics for a tenant
     *
     * @tags Log
     * @name V1TenantLogLineGetPointMetrics
     * @summary Get log point metrics
     * @request GET:/api/v1/stable/tenants/{tenant}/log-point-metrics
     * @secure
     */
    v1TenantLogLineGetPointMetrics: (tenant: string, query?: {
        /**
         * The start time to get logs for
         * @format date-time
         */
        since?: string;
        /**
         * The end time to get logs for
         * @format date-time
         */
        until?: string;
        /** A full-text search query to filter for */
        search?: string;
        /** The log level(s) to include */
        levels?: V1LogLineLevel[];
        /** The task external ID(s) to filter by */
        taskExternalIds?: string[];
        /** The workflow id(s) to filter for */
        workflow_ids?: string[];
        /** The step id(s) to filter for */
        step_ids?: string[];
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1LogsPointMetrics, any, {}>>;
    /**
     * @description Replay tasks
     *
     * @tags Task
     * @name V1TaskReplay
     * @summary Replay tasks
     * @request POST:/api/v1/stable/tenants/{tenant}/tasks/replay
     * @secure
     */
    v1TaskReplay: (tenant: string, data: V1ReplayTaskRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1ReplayedTasks, any, {}>>;
    /**
     * @description Restore an evicted durable task
     *
     * @tags Task
     * @name V1TaskRestore
     * @summary Restore a task
     * @request POST:/api/v1/stable/tasks/{task}/restore
     * @secure
     */
    v1TaskRestore: (task: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1RestoreTaskResponse, any, {}>>;
    /**
     * @description Lists all tasks that belong a specific list of dags
     *
     * @tags Task
     * @name V1DagListTasks
     * @summary List tasks
     * @request GET:/api/v1/stable/dags/tasks
     * @secure
     */
    v1DagListTasks: (query: {
        /** The external id of the DAG */
        dag_ids: string[];
        /**
         * The tenant id
         * @format uuid
         * @minLength 36
         * @maxLength 36
         */
        tenant: string;
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1DagChildren[], any, {}>>;
    /**
     * @description Lists workflow runs for a tenant.
     *
     * @tags Workflow Runs
     * @name V1WorkflowRunList
     * @summary List workflow runs
     * @request GET:/api/v1/stable/tenants/{tenant}/workflow-runs
     * @secure
     */
    v1WorkflowRunList: (tenant: string, query: {
        /**
         * The number to skip
         * @format int64
         */
        offset?: number;
        /**
         * The number to limit by
         * @format int64
         */
        limit?: number;
        /** A list of statuses to filter by */
        statuses?: V1TaskStatus[];
        /**
         * The earliest date to filter by
         * @format date-time
         */
        since: string;
        /**
         * The latest date to filter by
         * @format date-time
         */
        until?: string;
        /** Additional metadata k-v pairs to filter by */
        additional_metadata?: string[];
        /** How to combine multiple additional_metadata pairs. OR matches runs containing any pair, AND matches runs containing all pairs. Defaults to OR. */
        additional_metadata_operator?: V1AdditionalMetadataOperator;
        /** The workflow ids to find runs for */
        workflow_ids?: string[];
        /**
         * The worker id to filter by
         * @format uuid
         * @minLength 36
         * @maxLength 36
         */
        worker_id?: string;
        /** Whether to include DAGs or only to include tasks */
        only_tasks: boolean;
        /**
         * The parent task external id to filter by
         * @format uuid
         * @minLength 36
         * @maxLength 36
         */
        parent_task_external_id?: string;
        /**
         * The external id of the event that triggered the workflow run
         * @format uuid
         * @minLength 36
         * @maxLength 36
         */
        triggering_event_external_id?: string;
        /** A flag for whether or not to include the input and output payloads in the response. Defaults to `true` if unset. */
        include_payloads?: boolean;
        /** Filter within the RUNNING status bucket. ALL returns both on-worker and evicted tasks, ON_WORKER returns only tasks running on a worker, EVICTED returns only evicted tasks. Defaults to ALL. */
        running_filter?: V1RunningFilter;
        /** The idempotency key(s) to filter for */
        idempotency_keys?: string[];
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1TaskSummaryList, any, {}>>;
    /**
     * @description Lists displayable names of workflow runs for a tenant
     *
     * @tags Workflow Runs
     * @name V1WorkflowRunDisplayNamesList
     * @summary List workflow runs
     * @request GET:/api/v1/stable/tenants/{tenant}/workflow-runs/display-names
     * @secure
     */
    v1WorkflowRunDisplayNamesList: (tenant: string, query: {
        /** The external ids of the workflow runs to get display names for */
        external_ids: string[];
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1WorkflowRunDisplayNameList, any, {}>>;
    /**
     * @description Lists external ids for workflow runs matching filters
     *
     * @tags Workflow Runs
     * @name V1WorkflowRunExternalIdsList
     * @summary List workflow run external ids
     * @request GET:/api/v1/stable/tenants/{tenant}/workflow-runs/external-ids
     * @secure
     */
    v1WorkflowRunExternalIdsList: (tenant: string, query: {
        /** A list of statuses to filter by */
        statuses?: V1TaskStatus[];
        /**
         * The earliest date to filter by
         * @format date-time
         */
        since: string;
        /**
         * The latest date to filter by
         * @format date-time
         */
        until?: string;
        /** Additional metadata k-v pairs to filter by */
        additional_metadata?: string[];
        /** The workflow ids to find runs for */
        workflow_ids?: string[];
        /** Filter within the RUNNING status bucket. ALL returns both on-worker and evicted tasks, ON_WORKER returns only tasks running on a worker, EVICTED returns only evicted tasks. Defaults to ALL. */
        running_filter?: V1RunningFilter;
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1WorkflowRunExternalIdList, any, {}>>;
    /**
     * @description Trigger a new workflow run
     *
     * @tags Workflow Runs
     * @name V1WorkflowRunCreate
     * @summary Create workflow run
     * @request POST:/api/v1/stable/tenants/{tenant}/workflow-runs/trigger
     * @secure
     */
    v1WorkflowRunCreate: (tenant: string, data: V1TriggerWorkflowRunRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1WorkflowRunDetails, any, {}>>;
    /**
     * @description Branch a durable task from a specific node, creating a new branch and re-processing its matches.
     *
     * @tags Workflow Runs
     * @name V1DurableTaskBranch
     * @summary Branch durable task
     * @request POST:/api/v1/stable/tenants/{tenant}/durable-tasks/branch
     * @secure
     */
    v1DurableTaskBranch: (tenant: string, data: V1BranchDurableTaskRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1BranchDurableTaskResponse, any, {}>>;
    /**
     * @description Lists all event log entries for a durable task.
     *
     * @tags Durable Tasks
     * @name V1DurableTaskEventLogList
     * @summary List durable event log
     * @request GET:/api/v1/stable/tenants/{tenant}/durable-tasks/{durable-task}
     * @secure
     */
    v1DurableTaskEventLogList: (tenant: string, durableTask: string, query?: {
        /**
         * The number of event log entries to skip
         * @format int64
         */
        offset?: number;
        /**
         * The number of event log entries to limit by
         * @format int64
         */
        limit?: number;
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1DurableEventLogList, any, {}>>;
    /**
     * @description Get a durable stream payload uploaded ahead of its publish, by the payload_ref of the message that references it.
     *
     * @tags Streams
     * @name V1StreamPayloadGet
     * @summary Get stream payload
     * @request GET:/api/v1/stable/tenants/{tenant}/streams/payloads
     * @secure
     */
    v1StreamPayloadGet: (tenant: string, query: {
        /** The payload_ref of the stream message */
        ref: string;
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<File, any, {}>>;
    /**
     * @description Upload a payload too large for a gRPC publish, then publish the message with the returned ref as its payload_ref.
     *
     * @tags Streams
     * @name V1StreamPayloadUpload
     * @summary Upload stream payload
     * @request POST:/api/v1/stable/tenants/{tenant}/streams/payloads
     * @secure
     */
    v1StreamPayloadUpload: (tenant: string, data: {
        /** @format binary */
        payload: File;
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1StreamPayloadRef, any, {}>>;
    /**
     * @description Get a workflow run and its metadata to display on the "detail" page
     *
     * @tags Workflow Runs
     * @name V1WorkflowRunGet
     * @summary List tasks
     * @request GET:/api/v1/stable/workflow-runs/{v1-workflow-run}
     * @secure
     */
    v1WorkflowRunGet: (v1WorkflowRun: string, query?: {
        /** Whether to include the DAG orchestrator's task events, which are hidden by default */
        includeOrchestratorEvents?: boolean;
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1WorkflowRunDetails, any, {}>>;
    /**
     * @description Get the status of a workflow run.
     *
     * @tags Workflow Runs
     * @name V1WorkflowRunGetStatus
     * @summary Get workflow run status
     * @request GET:/api/v1/stable/workflow-runs/{v1-workflow-run}/status
     * @secure
     */
    v1WorkflowRunGetStatus: (v1WorkflowRun: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1TaskStatus, any, {}>>;
    /**
     * @description List all tasks for a workflow run
     *
     * @tags Workflow Runs
     * @name V1WorkflowRunTaskEventsList
     * @summary List tasks
     * @request GET:/api/v1/stable/workflow-runs/{v1-workflow-run}/task-events
     * @secure
     */
    v1WorkflowRunTaskEventsList: (v1WorkflowRun: string, query?: {
        /**
         * The number to skip
         * @format int64
         */
        offset?: number;
        /**
         * The number to limit by
         * @format int64
         */
        limit?: number;
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1TaskEventList, any, {}>>;
    /**
     * @description Get OTel trace for a workflow run
     *
     * @tags Observability
     * @name V1ObservabilityGetTrace
     * @summary Get OTel trace
     * @request GET:/api/v1/stable/tenants/{tenant}/traces
     * @secure
     */
    v1ObservabilityGetTrace: (tenant: string, query: {
        /**
         * The workflow run external id
         * @format uuid
         * @minLength 36
         * @maxLength 36
         */
        run_external_id: string;
        /**
         * The number of spans to skip
         * @format int64
         */
        offset?: number;
        /**
         * The number of spans to limit by
         * @format int64
         */
        limit?: number;
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<OtelSpanList, any, {}>>;
    /**
     * @description Get the timings for a workflow run
     *
     * @tags Workflow Runs
     * @name V1WorkflowRunGetTimings
     * @summary List timings for a workflow run
     * @request GET:/api/v1/stable/workflow-runs/{v1-workflow-run}/task-timings
     * @secure
     */
    v1WorkflowRunGetTimings: (v1WorkflowRun: string, query?: {
        /**
         * The depth to retrieve children
         * @format int64
         */
        depth?: number;
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1TaskTimingList, any, {}>>;
    /**
     * @description Get a summary of task run metrics for a tenant
     *
     * @tags Task
     * @name V1TaskListStatusMetrics
     * @summary Get task metrics
     * @request GET:/api/v1/stable/tenants/{tenant}/task-metrics
     * @secure
     */
    v1TaskListStatusMetrics: (tenant: string, query: {
        /**
         * The start time to get metrics for
         * @format date-time
         */
        since: string;
        /**
         * The end time to get metrics for
         * @format date-time
         */
        until?: string;
        /** The workflow id to find runs for */
        workflow_ids?: string[];
        /**
         * The parent task's external id
         * @format uuid
         * @minLength 36
         * @maxLength 36
         */
        parent_task_external_id?: string;
        /**
         * The id of the event that triggered the task
         * @format uuid
         * @minLength 36
         * @maxLength 36
         */
        triggering_event_external_id?: string;
        /** Additional metadata k-v pairs to filter by */
        additional_metadata?: string[];
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1TaskRunMetrics, any, {}>>;
    /**
     * @description Get a minute by minute breakdown of task metrics for a tenant
     *
     * @tags Task
     * @name V1TaskGetPointMetrics
     * @summary Get task point metrics
     * @request GET:/api/v1/stable/tenants/{tenant}/task-point-metrics
     * @secure
     */
    v1TaskGetPointMetrics: (tenant: string, query?: {
        /**
         * The time after the task was created
         * @format date-time
         * @example "2021-01-01T00:00:00Z"
         */
        createdAfter?: string;
        /**
         * The time before the task was completed
         * @format date-time
         * @example "2021-01-01T00:00:00Z"
         */
        finishedBefore?: string;
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1TaskPointMetrics, any, {}>>;
    /**
     * @description Lists all events for a tenant.
     *
     * @tags Event
     * @name V1EventList
     * @summary List events
     * @request GET:/api/v1/stable/tenants/{tenant}/events
     * @secure
     */
    v1EventList: (tenant: string, query?: {
        /**
         * The number to skip
         * @format int64
         */
        offset?: number;
        /**
         * The number to limit by
         * @format int64
         */
        limit?: number;
        /** A list of keys to filter by */
        keys?: EventKey[];
        /**
         * Consider events that occurred after this time
         * @format date-time
         */
        since?: string;
        /**
         * Consider events that occurred before this time
         * @format date-time
         */
        until?: string;
        /** Filter to events that are associated with a specific workflow run */
        workflowIds?: string[];
        /** Filter to events that are associated with workflow runs matching a certain status */
        workflowRunStatuses?: V1TaskStatus[];
        /** Filter to specific events by their ids */
        eventIds?: string[];
        /** Filter by additional metadata on the events */
        additionalMetadata?: string[];
        /** The scopes to filter by */
        scopes?: string[];
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1EventList, any, {}>>;
    /**
     * @description Get an event by its id
     *
     * @tags Event
     * @name V1EventGet
     * @summary Get events
     * @request GET:/api/v1/stable/tenants/{tenant}/events/{v1-event}
     * @secure
     */
    v1EventGet: (tenant: string, v1Event: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1Event, any, {}>>;
    /**
     * @description Lists all event keys for a tenant.
     *
     * @tags Event
     * @name V1EventKeyList
     * @summary List event keys
     * @request GET:/api/v1/stable/tenants/{tenant}/events/keys
     * @secure
     */
    v1EventKeyList: (tenant: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<EventKeyList, any, {}>>;
    /**
     * @description Lists all filters for a tenant.
     *
     * @tags Filter
     * @name V1FilterList
     * @summary List filters
     * @request GET:/api/v1/stable/tenants/{tenant}/filters
     * @secure
     */
    v1FilterList: (tenant: string, query?: {
        /**
         * The number to skip
         * @format int64
         */
        offset?: number;
        /**
         * The number to limit by
         * @format int64
         */
        limit?: number;
        /** The workflow ids to filter by */
        workflowIds?: string[];
        /** The scopes to subset candidate filters by */
        scopes?: string[];
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1FilterList, any, {}>>;
    /**
     * @description Create a new filter
     *
     * @tags Filter
     * @name V1FilterCreate
     * @summary Create a filter
     * @request POST:/api/v1/stable/tenants/{tenant}/filters
     * @secure
     */
    v1FilterCreate: (tenant: string, data: V1CreateFilterRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1Filter, any, {}>>;
    /**
     * @description Get a filter by its id
     *
     * @tags Filter
     * @name V1FilterGet
     * @summary Get a filter
     * @request GET:/api/v1/stable/tenants/{tenant}/filters/{v1-filter}
     * @secure
     */
    v1FilterGet: (tenant: string, v1Filter: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1Filter, any, {}>>;
    /**
     * @description Delete a filter
     *
     * @tags Filter
     * @name V1FilterDelete
     * @summary Delete a filter
     * @request DELETE:/api/v1/stable/tenants/{tenant}/filters/{v1-filter}
     * @secure
     */
    v1FilterDelete: (tenant: string, v1Filter: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1Filter, any, {}>>;
    /**
     * @description Update a filter
     *
     * @tags Filter
     * @name V1FilterUpdate
     * @summary Update a filter
     * @request PATCH:/api/v1/stable/tenants/{tenant}/filters/{v1-filter}
     * @secure
     */
    v1FilterUpdate: (tenant: string, v1Filter: string, data: V1UpdateFilterRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1Filter, any, {}>>;
    /**
     * @description Lists all webhook for a tenant.
     *
     * @tags Webhook
     * @name V1WebhookList
     * @summary List webhooks
     * @request GET:/api/v1/stable/tenants/{tenant}/webhooks
     * @secure
     */
    v1WebhookList: (tenant: string, query?: {
        /**
         * The number to skip
         * @format int64
         */
        offset?: number;
        /**
         * The number to limit by
         * @format int64
         */
        limit?: number;
        /** The source names to filter by */
        sourceNames?: V1WebhookSourceName[];
        /** The webhook names to filter by */
        webhookNames?: string[];
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1WebhookList, any, {}>>;
    /**
     * @description Create a new webhook
     *
     * @tags Webhook
     * @name V1WebhookCreate
     * @summary Create a webhook
     * @request POST:/api/v1/stable/tenants/{tenant}/webhooks
     * @secure
     */
    v1WebhookCreate: (tenant: string, data: V1CreateWebhookRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1Webhook, any, {}>>;
    /**
     * @description Get a webhook by its name
     *
     * @tags Webhook
     * @name V1WebhookGet
     * @summary Get a webhook
     * @request GET:/api/v1/stable/tenants/{tenant}/webhooks/{v1-webhook}
     * @secure
     */
    v1WebhookGet: (tenant: string, v1Webhook: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1Webhook, any, {}>>;
    /**
     * @description Delete a webhook
     *
     * @tags Webhook
     * @name V1WebhookDelete
     * @summary Delete a webhook
     * @request DELETE:/api/v1/stable/tenants/{tenant}/webhooks/{v1-webhook}
     * @secure
     */
    v1WebhookDelete: (tenant: string, v1Webhook: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1Webhook, any, {}>>;
    /**
     * @description Post an incoming webhook message
     *
     * @tags Webhook
     * @name V1WebhookReceive
     * @summary Post a webhook message
     * @request POST:/api/v1/stable/tenants/{tenant}/webhooks/{v1-webhook}
     */
    v1WebhookReceive: (tenant: string, v1Webhook: string, data?: any, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1WebhookResponse, any, {}>>;
    /**
     * @description Update a webhook
     *
     * @tags Webhook
     * @name V1WebhookUpdate
     * @summary Update a webhook
     * @request PATCH:/api/v1/stable/tenants/{tenant}/webhooks/{v1-webhook}
     * @secure
     */
    v1WebhookUpdate: (tenant: string, v1Webhook: string, data: V1UpdateWebhookRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1Webhook, any, {}>>;
    /**
     * @description Evaluate a CEL expression against provided input data.
     *
     * @tags CEL
     * @name V1CelDebug
     * @summary Debug a CEL expression
     * @request POST:/api/v1/stable/tenants/{tenant}/cel/debug
     * @secure
     */
    v1CelDebug: (tenant: string, data: V1CELDebugRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<V1CELDebugResponse, any, {}>>;
    /**
     * @description Gets the readiness status
     *
     * @tags Healthcheck
     * @name ReadinessGet
     * @summary Get readiness
     * @request GET:/api/ready
     */
    readinessGet: (params?: RequestParams) => Promise<import("axios").AxiosResponse<void, any, {}>>;
    /**
     * @description Gets the liveness status
     *
     * @tags Healthcheck
     * @name LivenessGet
     * @summary Get liveness
     * @request GET:/api/live
     */
    livenessGet: (params?: RequestParams) => Promise<import("axios").AxiosResponse<void, any, {}>>;
    /**
     * @description Gets metadata for the Hatchet instance
     *
     * @tags Metadata
     * @name MetadataGet
     * @summary Get metadata
     * @request GET:/api/v1/meta
     */
    metadataGet: (params?: RequestParams) => Promise<import("axios").AxiosResponse<APIMeta, any, {}>>;
    /**
     * @description Gets metadata for the Hatchet cloud instance
     *
     * @tags Metadata
     * @name CloudMetadataGet
     * @summary Get cloud metadata
     * @request GET:/api/v1/cloud/metadata
     */
    cloudMetadataGet: (params?: RequestParams) => Promise<import("axios").AxiosResponse<APIErrors, any, {}>>;
    /**
     * @description List all integrations
     *
     * @tags Metadata
     * @name MetadataListIntegrations
     * @summary List integrations
     * @request GET:/api/v1/meta/integrations
     * @secure
     */
    metadataListIntegrations: (params?: RequestParams) => Promise<import("axios").AxiosResponse<ListAPIMetaIntegration, any, {}>>;
    /**
     * @description Logs in a user.
     *
     * @tags User
     * @name UserUpdateLogin
     * @summary Login user
     * @request POST:/api/v1/users/login
     */
    userUpdateLogin: (data: UserLoginRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<User, any, {}>>;
    /**
     * @description Starts the OAuth flow
     *
     * @tags User
     * @name UserUpdateGoogleOauthStart
     * @summary Start OAuth flow
     * @request GET:/api/v1/users/google/start
     */
    userUpdateGoogleOauthStart: (params?: RequestParams) => Promise<import("axios").AxiosResponse<any, any, {}>>;
    /**
     * @description Completes the OAuth flow
     *
     * @tags User
     * @name UserUpdateGoogleOauthCallback
     * @summary Complete OAuth flow
     * @request GET:/api/v1/users/google/callback
     */
    userUpdateGoogleOauthCallback: (params?: RequestParams) => Promise<import("axios").AxiosResponse<any, any, {}>>;
    /**
     * @description Starts the OAuth flow
     *
     * @tags User
     * @name UserUpdateGithubOauthStart
     * @summary Start OAuth flow
     * @request GET:/api/v1/users/github/start
     */
    userUpdateGithubOauthStart: (params?: RequestParams) => Promise<import("axios").AxiosResponse<any, any, {}>>;
    /**
     * @description Completes the OAuth flow
     *
     * @tags User
     * @name UserUpdateGithubOauthCallback
     * @summary Complete OAuth flow
     * @request GET:/api/v1/users/github/callback
     */
    userUpdateGithubOauthCallback: (params?: RequestParams) => Promise<import("axios").AxiosResponse<any, any, {}>>;
    /**
     * @description Starts the OAuth flow
     *
     * @tags User
     * @name UserUpdateSlackOauthStart
     * @summary Start OAuth flow
     * @request GET:/api/v1/tenants/{tenant}/slack/start
     * @secure
     */
    userUpdateSlackOauthStart: (tenant: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<any, any, {}>>;
    /**
     * @description Completes the OAuth flow
     *
     * @tags User
     * @name UserUpdateSlackOauthCallback
     * @summary Complete OAuth flow
     * @request GET:/api/v1/users/slack/callback
     * @secure
     */
    userUpdateSlackOauthCallback: (params?: RequestParams) => Promise<import("axios").AxiosResponse<any, any, {}>>;
    /**
     * @description SNS event
     *
     * @tags Github
     * @name SnsUpdate
     * @summary Github app tenant webhook
     * @request POST:/api/v1/sns/{tenant}/{event}
     */
    snsUpdate: (tenant: string, event: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<void, any, {}>>;
    /**
     * @description List SNS integrations
     *
     * @tags SNS
     * @name SnsList
     * @summary List SNS integrations
     * @request GET:/api/v1/tenants/{tenant}/sns
     * @secure
     */
    snsList: (tenant: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<ListSNSIntegrations, any, {}>>;
    /**
     * @description Create SNS integration
     *
     * @tags SNS
     * @name SnsCreate
     * @summary Create SNS integration
     * @request POST:/api/v1/tenants/{tenant}/sns
     * @secure
     */
    snsCreate: (tenant: string, data: CreateSNSIntegrationRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<SNSIntegration, any, {}>>;
    /**
     * @description Creates a new tenant alert email group
     *
     * @tags Tenant
     * @name AlertEmailGroupCreate
     * @summary Create tenant alert email group
     * @request POST:/api/v1/tenants/{tenant}/alerting-email-groups
     * @secure
     */
    alertEmailGroupCreate: (tenant: string, data: CreateTenantAlertEmailGroupRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<TenantAlertEmailGroup, any, {}>>;
    /**
     * @description Gets a list of tenant alert email groups
     *
     * @tags Tenant
     * @name AlertEmailGroupList
     * @summary List tenant alert email groups
     * @request GET:/api/v1/tenants/{tenant}/alerting-email-groups
     * @secure
     */
    alertEmailGroupList: (tenant: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<TenantAlertEmailGroupList, any, {}>>;
    /**
     * @description Gets the resource policy for a tenant
     *
     * @tags Tenant
     * @name TenantResourcePolicyGet
     * @summary Create tenant alert email group
     * @request GET:/api/v1/tenants/{tenant}/resource-policy
     * @secure
     */
    tenantResourcePolicyGet: (tenant: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<TenantResourcePolicy, any, {}>>;
    /**
     * @description Updates a tenant alert email group
     *
     * @tags Tenant
     * @name AlertEmailGroupUpdate
     * @summary Update tenant alert email group
     * @request PATCH:/api/v1/alerting-email-groups/{alert-email-group}
     * @secure
     */
    alertEmailGroupUpdate: (alertEmailGroup: string, data: UpdateTenantAlertEmailGroupRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<TenantAlertEmailGroup, any, {}>>;
    /**
     * @description Deletes a tenant alert email group
     *
     * @tags Tenant
     * @name AlertEmailGroupDelete
     * @summary Delete tenant alert email group
     * @request DELETE:/api/v1/alerting-email-groups/{alert-email-group}
     * @secure
     */
    alertEmailGroupDelete: (alertEmailGroup: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<void, any, {}>>;
    /**
     * @description Delete SNS integration
     *
     * @tags SNS
     * @name SnsDelete
     * @summary Delete SNS integration
     * @request DELETE:/api/v1/sns/{sns}
     * @secure
     */
    snsDelete: (sns: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<void, any, {}>>;
    /**
     * @description List Slack webhooks
     *
     * @tags Slack
     * @name SlackWebhookList
     * @summary List Slack integrations
     * @request GET:/api/v1/tenants/{tenant}/slack
     * @secure
     */
    slackWebhookList: (tenant: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<ListSlackWebhooks, any, {}>>;
    /**
     * @description Delete Slack webhook
     *
     * @tags Slack
     * @name SlackWebhookDelete
     * @summary Delete Slack webhook
     * @request DELETE:/api/v1/slack/{slack}
     * @secure
     */
    slackWebhookDelete: (slack: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<void, any, {}>>;
    /**
     * @description Gets the current user
     *
     * @tags User
     * @name UserGetCurrent
     * @summary Get current user
     * @request GET:/api/v1/users/current
     * @secure
     */
    userGetCurrent: (params?: RequestParams) => Promise<import("axios").AxiosResponse<User, any, {}>>;
    /**
     * @description Update a user password.
     *
     * @tags User
     * @name UserUpdatePassword
     * @summary Change user password
     * @request POST:/api/v1/users/password
     * @secure
     */
    userUpdatePassword: (data: UserChangePasswordRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<User, any, {}>>;
    /**
     * @description Registers a user.
     *
     * @tags User
     * @name UserCreate
     * @summary Register user
     * @request POST:/api/v1/users/register
     */
    userCreate: (data: UserRegisterRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<User, any, {}>>;
    /**
     * @description Logs out a user.
     *
     * @tags User
     * @name UserUpdateLogout
     * @summary Logout user
     * @request POST:/api/v1/users/logout
     * @secure
     */
    userUpdateLogout: (params?: RequestParams) => Promise<import("axios").AxiosResponse<User, any, {}>>;
    /**
     * @description Lists all tenant memberships for the current user
     *
     * @tags User
     * @name TenantMembershipsList
     * @summary List tenant memberships
     * @request GET:/api/v1/users/memberships
     * @secure
     */
    tenantMembershipsList: (params?: RequestParams) => Promise<import("axios").AxiosResponse<UserTenantMembershipsList, any, {}>>;
    /**
     * @description Lists all tenant invites for the current user
     *
     * @tags Tenant
     * @name UserListTenantInvites
     * @summary List tenant invites
     * @request GET:/api/v1/users/invites
     * @secure
     */
    userListTenantInvites: (params?: RequestParams) => Promise<import("axios").AxiosResponse<TenantInviteList, any, {}>>;
    /**
     * @description Accepts a tenant invite
     *
     * @tags Tenant
     * @name TenantInviteAccept
     * @summary Accept tenant invite
     * @request POST:/api/v1/users/invites/accept
     * @secure
     */
    tenantInviteAccept: (data: AcceptInviteRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<void, any, {}>>;
    /**
     * @description Rejects a tenant invite
     *
     * @tags Tenant
     * @name TenantInviteReject
     * @summary Reject tenant invite
     * @request POST:/api/v1/users/invites/reject
     * @secure
     */
    tenantInviteReject: (data: RejectInviteRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<void, any, {}>>;
    /**
     * @description Creates a new tenant
     *
     * @tags Tenant
     * @name TenantCreate
     * @summary Create tenant
     * @request POST:/api/v1/tenants
     * @secure
     */
    tenantCreate: (data: CreateTenantRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<Tenant, any, {}>>;
    /**
     * @description Update an existing tenant
     *
     * @tags Tenant
     * @name TenantUpdate
     * @summary Update tenant
     * @request PATCH:/api/v1/tenants/{tenant}
     * @secure
     */
    tenantUpdate: (tenant: string, data: UpdateTenantRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<Tenant, any, {}>>;
    /**
     * @description Get the details of a tenant
     *
     * @tags Tenant
     * @name TenantGet
     * @summary Get tenant
     * @request GET:/api/v1/tenants/{tenant}
     * @secure
     */
    tenantGet: (tenant: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<Tenant, any, {}>>;
    /**
     * @description Gets the alerting settings for a tenant
     *
     * @tags Tenant
     * @name TenantAlertingSettingsGet
     * @summary Get tenant alerting settings
     * @request GET:/api/v1/tenants/{tenant}/alerting/settings
     * @secure
     */
    tenantAlertingSettingsGet: (tenant: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<TenantAlertingSettings, any, {}>>;
    /**
     * @description Creates a new tenant invite
     *
     * @tags Tenant
     * @name TenantInviteCreate
     * @summary Create tenant invite
     * @request POST:/api/v1/tenants/{tenant}/invites
     * @secure
     */
    tenantInviteCreate: (tenant: string, data: CreateTenantInviteRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<TenantInvite, any, {}>>;
    /**
     * @description Gets a list of tenant invites
     *
     * @tags Tenant
     * @name TenantInviteList
     * @summary List tenant invites
     * @request GET:/api/v1/tenants/{tenant}/invites
     * @secure
     */
    tenantInviteList: (tenant: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<TenantInviteList, any, {}>>;
    /**
     * @description Updates a tenant invite
     *
     * @name TenantInviteUpdate
     * @summary Update invite
     * @request PATCH:/api/v1/tenants/{tenant}/invites/{tenant-invite}
     * @secure
     */
    tenantInviteUpdate: (tenant: string, tenantInvite: string, data: UpdateTenantInviteRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<TenantInvite, any, {}>>;
    /**
     * @description Deletes a tenant invite
     *
     * @name TenantInviteDelete
     * @summary Delete invite
     * @request DELETE:/api/v1/tenants/{tenant}/invites/{tenant-invite}
     * @secure
     */
    tenantInviteDelete: (tenant: string, tenantInvite: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<TenantInvite, any, {}>>;
    /**
     * @description Create an API token for a tenant
     *
     * @tags API Token
     * @name ApiTokenCreate
     * @summary Create API Token
     * @request POST:/api/v1/tenants/{tenant}/api-tokens
     * @secure
     */
    apiTokenCreate: (tenant: string, data: CreateAPITokenRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<CreateAPITokenResponse, any, {}>>;
    /**
     * @description List API tokens for a tenant
     *
     * @tags API Token
     * @name ApiTokenList
     * @summary List API Tokens
     * @request GET:/api/v1/tenants/{tenant}/api-tokens
     * @secure
     */
    apiTokenList: (tenant: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<ListAPITokensResponse, any, {}>>;
    /**
     * @description Revoke an API token for a tenant
     *
     * @tags API Token
     * @name ApiTokenUpdateRevoke
     * @summary Revoke API Token
     * @request POST:/api/v1/api-tokens/{api-token}
     * @secure
     */
    apiTokenUpdateRevoke: (apiToken: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<void, any, {}>>;
    /**
     * @description Get the queue metrics for the tenant
     *
     * @tags Workflow
     * @name TenantGetQueueMetrics
     * @summary Get workflow metrics
     * @request GET:/api/v1/tenants/{tenant}/queue-metrics
     * @secure
     */
    tenantGetQueueMetrics: (tenant: string, query?: {
        /** A list of workflow IDs to filter by */
        workflows?: WorkflowID[];
        /**
         * A list of metadata key value pairs to filter by
         * @example ["key1:value1","key2:value2"]
         */
        additionalMetadata?: string[];
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<TenantQueueMetrics, any, {}>>;
    /**
     * @description Get the queue metrics for the tenant
     *
     * @tags Tenant
     * @name TenantGetStepRunQueueMetrics
     * @summary Get step run metrics
     * @request GET:/api/v1/tenants/{tenant}/step-run-queue-metrics
     * @secure
     */
    tenantGetStepRunQueueMetrics: (tenant: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<TenantStepRunQueueMetrics, any, {}>>;
    /**
     * @description Lists all events for a tenant.
     *
     * @tags Event
     * @name EventList
     * @summary List events
     * @request GET:/api/v1/tenants/{tenant}/events
     * @secure
     */
    eventList: (tenant: string, query?: {
        /**
         * The number to skip
         * @format int64
         */
        offset?: number;
        /**
         * The number to limit by
         * @format int64
         */
        limit?: number;
        /** A list of keys to filter by */
        keys?: EventKey[];
        /** A list of workflow IDs to filter by */
        workflows?: WorkflowID[];
        /** A list of workflow run statuses to filter by */
        statuses?: WorkflowRunStatusList;
        /** The search query to filter for */
        search?: EventSearch;
        /** What to order by */
        orderByField?: EventOrderByField;
        /** The order direction */
        orderByDirection?: EventOrderByDirection;
        /**
         * A list of metadata key value pairs to filter by
         * @example ["key1:value1","key2:value2"]
         */
        additionalMetadata?: string[];
        /** A list of event ids to filter by */
        eventIds?: string[];
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<EventList, any, {}>>;
    /**
     * @description Creates a new event.
     *
     * @tags Event
     * @name EventCreate
     * @summary Create event
     * @request POST:/api/v1/tenants/{tenant}/events
     * @secure
     */
    eventCreate: (tenant: string, data: CreateEventRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<Event, any, {}>>;
    /**
     * @description Bulk creates new events.
     *
     * @tags Event
     * @name EventCreateBulk
     * @summary Bulk Create events
     * @request POST:/api/v1/tenants/{tenant}/events/bulk
     * @secure
     */
    eventCreateBulk: (tenant: string, data: BulkCreateEventRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<Events, any, {}>>;
    /**
     * @description Replays a list of events.
     *
     * @tags Event
     * @name EventUpdateReplay
     * @summary Replay events
     * @request POST:/api/v1/tenants/{tenant}/events/replay
     * @secure
     */
    eventUpdateReplay: (tenant: string, data: ReplayEventRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<EventList, any, {}>>;
    /**
     * @description Cancels all runs for a list of events.
     *
     * @tags Event
     * @name EventUpdateCancel
     * @summary Replay events
     * @request POST:/api/v1/tenants/{tenant}/events/cancel
     * @secure
     */
    eventUpdateCancel: (tenant: string, data: CancelEventRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<{
        workflowRunIds?: string[];
    }, any, {}>>;
    /**
     * @description Lists all rate limits for a tenant.
     *
     * @tags Rate Limits
     * @name RateLimitList
     * @summary List rate limits
     * @request GET:/api/v1/tenants/{tenant}/rate-limits
     * @secure
     */
    rateLimitList: (tenant: string, query?: {
        /**
         * The number to skip
         * @format int64
         */
        offset?: number;
        /**
         * The number to limit by
         * @format int64
         */
        limit?: number;
        /** The search query to filter for */
        search?: string;
        /** What to order by */
        orderByField?: RateLimitOrderByField;
        /** The order direction */
        orderByDirection?: RateLimitOrderByDirection;
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<RateLimitList, any, {}>>;
    /**
     * @description Delete a rate limit for a tenant.
     *
     * @tags Rate Limits
     * @name RateLimitDelete
     * @summary Delete rate limit
     * @request DELETE:/api/v1/tenants/{tenant}/rate-limits
     * @secure
     */
    rateLimitDelete: (tenant: string, query: {
        /** The limit key */
        key: string;
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<void, any, {}>>;
    /**
     * @description Gets a list of tenant members
     *
     * @tags Tenant
     * @name TenantMemberList
     * @summary List tenant members
     * @request GET:/api/v1/tenants/{tenant}/members
     * @secure
     */
    tenantMemberList: (tenant: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<TenantMemberList, any, {}>>;
    /**
     * @description Update a tenant member
     *
     * @tags Tenant
     * @name TenantMemberUpdate
     * @summary Update a tenant member
     * @request PATCH:/api/v1/tenants/{tenant}/members/{member}
     * @secure
     */
    tenantMemberUpdate: (tenant: string, member: string, data: UpdateTenantMemberRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<TenantMember, any, {}>>;
    /**
     * @description Delete a member from a tenant
     *
     * @tags Tenant
     * @name TenantMemberDelete
     * @summary Delete a tenant member
     * @request DELETE:/api/v1/tenants/{tenant}/members/{member}
     * @secure
     */
    tenantMemberDelete: (tenant: string, member: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<TenantMember, any, {}>>;
    /**
     * @description Get an event.
     *
     * @tags Event
     * @name EventGet
     * @summary Get event data
     * @request GET:/api/v1/events/{event}
     * @secure
     */
    eventGet: (event: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<Event, any, {}>>;
    /**
     * @description Get the data for an event.
     *
     * @tags Event
     * @name EventDataGet
     * @summary Get event data
     * @request GET:/api/v1/events/{event}/data
     * @secure
     */
    eventDataGet: (event: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<EventData, any, {}>>;
    /**
     * @description Get the data for an event.
     *
     * @tags Event
     * @name EventDataGetWithTenant
     * @summary Get event data
     * @request GET:/api/v1/tenants/{tenant}/events/{event-with-tenant}/data
     * @secure
     */
    eventDataGetWithTenant: (eventWithTenant: string, tenant: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<EventData, any, {}>>;
    /**
     * @description Lists all event keys for a tenant.
     *
     * @tags Event
     * @name EventKeyList
     * @summary List event keys
     * @request GET:/api/v1/tenants/{tenant}/events/keys
     * @secure
     */
    eventKeyList: (tenant: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<EventKeyList, any, {}>>;
    /**
     * @description Get all workflows for a tenant
     *
     * @tags Workflow
     * @name WorkflowList
     * @summary Get workflows
     * @request GET:/api/v1/tenants/{tenant}/workflows
     * @secure
     */
    workflowList: (tenant: string, query?: {
        /**
         * The number to skip
         * @format int
         * @default 0
         */
        offset?: number;
        /**
         * The number to limit by
         * @format int
         * @default 50
         */
        limit?: number;
        /** Search by name */
        name?: string;
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<WorkflowList, any, {}>>;
    /**
     * @description Schedule a new workflow run for a tenant
     *
     * @tags Workflow Run
     * @name ScheduledWorkflowRunCreate
     * @summary Trigger workflow run
     * @request POST:/api/v1/tenants/{tenant}/workflows/{workflow}/scheduled
     * @secure
     */
    scheduledWorkflowRunCreate: (tenant: string, workflow: string, data: ScheduleWorkflowRunRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<ScheduledWorkflows, any, {}>>;
    /**
     * @description Get all scheduled workflow runs for a tenant
     *
     * @tags Workflow
     * @name WorkflowScheduledList
     * @summary Get scheduled workflow runs
     * @request GET:/api/v1/tenants/{tenant}/workflows/scheduled
     * @secure
     */
    workflowScheduledList: (tenant: string, query?: {
        /**
         * The number to skip
         * @format int64
         */
        offset?: number;
        /**
         * The number to limit by
         * @format int64
         */
        limit?: number;
        /** The order by field */
        orderByField?: ScheduledWorkflowsOrderByField;
        /** The order by direction */
        orderByDirection?: WorkflowRunOrderByDirection;
        /**
         * The workflow id to get runs for.
         * @format uuid
         * @minLength 36
         * @maxLength 36
         */
        workflowId?: string;
        /**
         * The parent workflow run id
         * @format uuid
         * @minLength 36
         * @maxLength 36
         */
        parentWorkflowRunId?: string;
        /**
         * The parent step run id
         * @format uuid
         * @minLength 36
         * @maxLength 36
         */
        parentStepRunId?: string;
        /**
         * A list of metadata key value pairs to filter by
         * @example ["key1:value1","key2:value2"]
         */
        additionalMetadata?: string[];
        /** A list of scheduled run statuses to filter by */
        statuses?: ScheduledRunStatus[];
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<ScheduledWorkflowsList, any, {}>>;
    /**
     * @description Get a scheduled workflow run for a tenant
     *
     * @tags Workflow
     * @name WorkflowScheduledGet
     * @summary Get scheduled workflow run
     * @request GET:/api/v1/tenants/{tenant}/workflows/scheduled/{scheduled-workflow-run}
     * @secure
     */
    workflowScheduledGet: (tenant: string, scheduledWorkflowRun: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<ScheduledWorkflows, any, {}>>;
    /**
     * @description Delete a scheduled workflow run for a tenant
     *
     * @tags Workflow
     * @name WorkflowScheduledDelete
     * @summary Delete scheduled workflow run
     * @request DELETE:/api/v1/tenants/{tenant}/workflows/scheduled/{scheduled-workflow-run}
     * @secure
     */
    workflowScheduledDelete: (tenant: string, scheduledWorkflowRun: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<void, any, {}>>;
    /**
     * @description Update (reschedule) a scheduled workflow run for a tenant
     *
     * @tags Workflow
     * @name WorkflowScheduledUpdate
     * @summary Update scheduled workflow run
     * @request PATCH:/api/v1/tenants/{tenant}/workflows/scheduled/{scheduled-workflow-run}
     * @secure
     */
    workflowScheduledUpdate: (tenant: string, scheduledWorkflowRun: string, data: UpdateScheduledWorkflowRunRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<ScheduledWorkflows, any, {}>>;
    /**
     * @description Trigger a scheduled workflow run immediately for a tenant
     *
     * @tags Workflow
     * @name WorkflowScheduledTrigger
     * @summary Trigger scheduled workflow run
     * @request POST:/api/v1/tenants/{tenant}/workflows/scheduled/{scheduled-workflow-run}
     * @secure
     */
    workflowScheduledTrigger: (tenant: string, scheduledWorkflowRun: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<TriggerRunResult, any, {}>>;
    /**
     * @description Bulk delete scheduled workflow runs for a tenant
     *
     * @tags Workflow
     * @name WorkflowScheduledBulkDelete
     * @summary Bulk delete scheduled workflow runs
     * @request POST:/api/v1/tenants/{tenant}/workflows/scheduled/bulk-delete
     * @secure
     */
    workflowScheduledBulkDelete: (tenant: string, data: ScheduledWorkflowsBulkDeleteRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<ScheduledWorkflowsBulkDeleteResponse, any, {}>>;
    /**
     * @description Bulk update (reschedule) scheduled workflow runs for a tenant
     *
     * @tags Workflow
     * @name WorkflowScheduledBulkUpdate
     * @summary Bulk update scheduled workflow runs
     * @request POST:/api/v1/tenants/{tenant}/workflows/scheduled/bulk-update
     * @secure
     */
    workflowScheduledBulkUpdate: (tenant: string, data: ScheduledWorkflowsBulkUpdateRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<ScheduledWorkflowsBulkUpdateResponse, any, {}>>;
    /**
     * @description Create a new cron job workflow trigger for a tenant
     *
     * @tags Workflow Run
     * @name CronWorkflowTriggerCreate
     * @summary Create cron job workflow trigger
     * @request POST:/api/v1/tenants/{tenant}/workflows/{workflow}/crons
     * @secure
     */
    cronWorkflowTriggerCreate: (tenant: string, workflow: string, data: CreateCronWorkflowTriggerRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<CronWorkflows, any, {}>>;
    /**
     * @description Get all cron job workflow triggers for a tenant
     *
     * @tags Workflow
     * @name CronWorkflowList
     * @summary Get cron job workflows
     * @request GET:/api/v1/tenants/{tenant}/workflows/crons
     * @secure
     */
    cronWorkflowList: (tenant: string, query?: {
        /**
         * The number to skip
         * @format int64
         */
        offset?: number;
        /**
         * The number to limit by
         * @format int64
         */
        limit?: number;
        /**
         * The workflow id to get runs for.
         * @format uuid
         * @minLength 36
         * @maxLength 36
         */
        workflowId?: string;
        /** The workflow name to get runs for. */
        workflowName?: string;
        /** The cron name to get runs for. */
        cronName?: string;
        /**
         * A list of metadata key value pairs to filter by
         * @example ["key1:value1","key2:value2"]
         */
        additionalMetadata?: string[];
        /** The order by field */
        orderByField?: CronWorkflowsOrderByField;
        /** The order by direction */
        orderByDirection?: WorkflowRunOrderByDirection;
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<CronWorkflowsList, any, {}>>;
    /**
     * @description Get a cron job workflow run for a tenant
     *
     * @tags Workflow
     * @name WorkflowCronGet
     * @summary Get cron job workflow run
     * @request GET:/api/v1/tenants/{tenant}/workflows/crons/{cron-workflow}
     * @secure
     */
    workflowCronGet: (tenant: string, cronWorkflow: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<CronWorkflows, any, {}>>;
    /**
     * @description Delete a cron job workflow run for a tenant
     *
     * @tags Workflow
     * @name WorkflowCronDelete
     * @summary Delete cron job workflow run
     * @request DELETE:/api/v1/tenants/{tenant}/workflows/crons/{cron-workflow}
     * @secure
     */
    workflowCronDelete: (tenant: string, cronWorkflow: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<void, any, {}>>;
    /**
     * @description Update a cron workflow for a tenant
     *
     * @tags Workflow
     * @name WorkflowCronUpdate
     * @summary Update cron job workflow run
     * @request PATCH:/api/v1/tenants/{tenant}/workflows/crons/{cron-workflow}
     * @secure
     */
    workflowCronUpdate: (tenant: string, cronWorkflow: string, data: UpdateCronWorkflowTriggerRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<void, any, {}>>;
    /**
     * @description Trigger a cron workflow immediately for a tenant
     *
     * @tags Workflow
     * @name WorkflowCronTrigger
     * @summary Trigger cron job workflow run immediately
     * @request POST:/api/v1/tenants/{tenant}/workflows/crons/{cron-workflow}
     * @secure
     */
    workflowCronTrigger: (tenant: string, cronWorkflow: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<TriggerRunResult, any, {}>>;
    /**
     * @description Cancel a batch of workflow runs
     *
     * @tags Workflow Run
     * @name WorkflowRunCancel
     * @summary Cancel workflow runs
     * @request POST:/api/v1/tenants/{tenant}/workflows/cancel
     * @secure
     */
    workflowRunCancel: (tenant: string, data: WorkflowRunsCancelRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<{
        workflowRunIds?: string[];
    }, any, {}>>;
    /**
     * @description Get a workflow for a tenant
     *
     * @tags Workflow
     * @name WorkflowGet
     * @summary Get workflow
     * @request GET:/api/v1/workflows/{workflow}
     * @secure
     */
    workflowGet: (workflow: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<Workflow, any, {}>>;
    /**
     * @description Delete a workflow for a tenant
     *
     * @tags Workflow
     * @name WorkflowDelete
     * @summary Delete workflow
     * @request DELETE:/api/v1/workflows/{workflow}
     * @secure
     */
    workflowDelete: (workflow: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<void, any, {}>>;
    /**
     * @description Update a workflow for a tenant
     *
     * @tags Workflow
     * @name WorkflowUpdate
     * @summary Update workflow
     * @request PATCH:/api/v1/workflows/{workflow}
     * @secure
     */
    workflowUpdate: (workflow: string, data: WorkflowUpdateRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<Workflow, any, {}>>;
    /**
     * @description Get a workflow version for a tenant
     *
     * @tags Workflow
     * @name WorkflowVersionGet
     * @summary Get workflow version
     * @request GET:/api/v1/workflows/{workflow}/versions
     * @secure
     */
    workflowVersionGet: (workflow: string, query?: {
        /**
         * The workflow version. If not supplied, the latest version is fetched.
         * @format uuid
         * @minLength 36
         * @maxLength 36
         */
        version?: string;
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<WorkflowVersion, any, {}>>;
    /**
     * @description Trigger a new workflow run for a tenant
     *
     * @tags Workflow Run
     * @name WorkflowRunCreate
     * @summary Trigger workflow run
     * @request POST:/api/v1/workflows/{workflow}/trigger
     * @secure
     */
    workflowRunCreate: (workflow: string, data: TriggerWorkflowRunRequest, query?: {
        /**
         * The workflow version. If not supplied, the latest version is fetched.
         * @format uuid
         * @minLength 36
         * @maxLength 36
         */
        version?: string;
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<WorkflowRun, any, {}>>;
    /**
     * @description Get the metrics for a workflow version
     *
     * @tags Workflow
     * @name WorkflowGetMetrics
     * @summary Get workflow metrics
     * @request GET:/api/v1/workflows/{workflow}/metrics
     * @secure
     */
    workflowGetMetrics: (workflow: string, query?: {
        /** A status of workflow run statuses to filter by */
        status?: WorkflowRunStatus;
        /** A group key to filter metrics by */
        groupKey?: string;
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<WorkflowMetrics, any, {}>>;
    /**
     * @description List events for a step run
     *
     * @tags Step Run
     * @name StepRunListEvents
     * @summary List events for step run
     * @request GET:/api/v1/step-runs/{step-run}/events
     * @secure
     */
    stepRunListEvents: (stepRun: string, query?: {
        /**
         * The number to skip
         * @format int64
         */
        offset?: number;
        /**
         * The number to limit by
         * @format int64
         */
        limit?: number;
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<StepRunEventList, any, {}>>;
    /**
     * @description List events for all step runs for a workflow run
     *
     * @tags Step Run
     * @name WorkflowRunListStepRunEvents
     * @summary List events for all step runs for a workflow run
     * @request GET:/api/v1/tenants/{tenant}/workflow-runs/{workflow-run}/step-run-events
     * @secure
     */
    workflowRunListStepRunEvents: (tenant: string, workflowRun: string, query?: {
        /**
         * Last ID of the last event
         * @format int32
         */
        lastId?: number;
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<StepRunEventList, any, {}>>;
    /**
     * @description List archives for a step run
     *
     * @tags Step Run
     * @name StepRunListArchives
     * @summary List archives for step run
     * @request GET:/api/v1/step-runs/{step-run}/archives
     * @secure
     */
    stepRunListArchives: (stepRun: string, query?: {
        /**
         * The number to skip
         * @format int64
         */
        offset?: number;
        /**
         * The number to limit by
         * @format int64
         */
        limit?: number;
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<StepRunArchiveList, any, {}>>;
    /**
     * @description Get a count of the workers available for workflow
     *
     * @tags Workflow
     * @name WorkflowGetWorkersCount
     * @summary Get workflow worker count
     * @request GET:/api/v1/tenants/{tenant}/workflows/{workflow}/worker-count
     * @secure
     */
    workflowGetWorkersCount: (tenant: string, workflow: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<WorkflowWorkersCount, any, {}>>;
    /**
     * @description Get all workflow runs for a tenant
     *
     * @tags Workflow
     * @name WorkflowRunList
     * @summary Get workflow runs
     * @request GET:/api/v1/tenants/{tenant}/workflows/runs
     * @secure
     */
    workflowRunList: (tenant: string, query?: {
        /**
         * The number to skip
         * @format int64
         */
        offset?: number;
        /**
         * The number to limit by
         * @format int64
         */
        limit?: number;
        /**
         * The event id to get runs for.
         * @format uuid
         * @minLength 36
         * @maxLength 36
         */
        eventId?: string;
        /**
         * The workflow id to get runs for.
         * @format uuid
         * @minLength 36
         * @maxLength 36
         */
        workflowId?: string;
        /**
         * The parent workflow run id
         * @format uuid
         * @minLength 36
         * @maxLength 36
         */
        parentWorkflowRunId?: string;
        /**
         * The parent step run id
         * @format uuid
         * @minLength 36
         * @maxLength 36
         */
        parentStepRunId?: string;
        /** A list of workflow run statuses to filter by */
        statuses?: WorkflowRunStatusList;
        /** A list of workflow kinds to filter by */
        kinds?: WorkflowKindList;
        /**
         * A list of metadata key value pairs to filter by
         * @example ["key1:value1","key2:value2"]
         */
        additionalMetadata?: string[];
        /**
         * The time after the workflow run was created
         * @format date-time
         * @example "2021-01-01T00:00:00Z"
         */
        createdAfter?: string;
        /**
         * The time before the workflow run was created
         * @format date-time
         * @example "2021-01-01T00:00:00Z"
         */
        createdBefore?: string;
        /**
         * The time after the workflow run was finished
         * @format date-time
         * @example "2021-01-01T00:00:00Z"
         */
        finishedAfter?: string;
        /**
         * The time before the workflow run was finished
         * @format date-time
         * @example "2021-01-01T00:00:00Z"
         */
        finishedBefore?: string;
        /** The order by field */
        orderByField?: WorkflowRunOrderByField;
        /** The order by direction */
        orderByDirection?: WorkflowRunOrderByDirection;
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<WorkflowRunList, any, {}>>;
    /**
     * @description Replays a list of workflow runs.
     *
     * @tags Workflow Run
     * @name WorkflowRunUpdateReplay
     * @summary Replay workflow runs
     * @request POST:/api/v1/tenants/{tenant}/workflow-runs/replay
     * @secure
     */
    workflowRunUpdateReplay: (tenant: string, data: ReplayWorkflowRunsRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<ReplayWorkflowRunsResponse, any, {}>>;
    /**
     * @description Get a summary of  workflow run metrics for a tenant
     *
     * @tags Workflow
     * @name WorkflowRunGetMetrics
     * @summary Get workflow runs metrics
     * @request GET:/api/v1/tenants/{tenant}/workflows/runs/metrics
     * @secure
     */
    workflowRunGetMetrics: (tenant: string, query?: {
        /**
         * The event id to get runs for.
         * @format uuid
         * @minLength 36
         * @maxLength 36
         */
        eventId?: string;
        /**
         * The workflow id to get runs for.
         * @format uuid
         * @minLength 36
         * @maxLength 36
         */
        workflowId?: string;
        /**
         * The parent workflow run id
         * @format uuid
         * @minLength 36
         * @maxLength 36
         */
        parentWorkflowRunId?: string;
        /**
         * The parent step run id
         * @format uuid
         * @minLength 36
         * @maxLength 36
         */
        parentStepRunId?: string;
        /**
         * A list of metadata key value pairs to filter by
         * @example ["key1:value1","key2:value2"]
         */
        additionalMetadata?: string[];
        /**
         * The time after the workflow run was created
         * @format date-time
         * @example "2021-01-01T00:00:00Z"
         */
        createdAfter?: string;
        /**
         * The time before the workflow run was created
         * @format date-time
         * @example "2021-01-01T00:00:00Z"
         */
        createdBefore?: string;
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<WorkflowRunsMetrics, any, {}>>;
    /**
     * @description Get a workflow run for a tenant
     *
     * @tags Workflow
     * @name WorkflowRunGet
     * @summary Get workflow run
     * @request GET:/api/v1/tenants/{tenant}/workflow-runs/{workflow-run}
     * @secure
     */
    workflowRunGet: (tenant: string, workflowRun: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<WorkflowRun, any, {}>>;
    /**
     * @description Get a workflow run for a tenant
     *
     * @tags Workflow
     * @name WorkflowRunGetShape
     * @summary Get workflow run
     * @request GET:/api/v1/tenants/{tenant}/workflow-runs/{workflow-run}/shape
     * @secure
     */
    workflowRunGetShape: (tenant: string, workflowRun: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<WorkflowRunShape, any, {}>>;
    /**
     * @description Get a step run by id
     *
     * @tags Step Run
     * @name StepRunGet
     * @summary Get step run
     * @request GET:/api/v1/tenants/{tenant}/step-runs/{step-run}
     * @secure
     */
    stepRunGet: (tenant: string, stepRun: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<StepRun, any, {}>>;
    /**
     * @description Reruns a step run
     *
     * @tags Step Run
     * @name StepRunUpdateRerun
     * @summary Rerun step run
     * @request POST:/api/v1/tenants/{tenant}/step-runs/{step-run}/rerun
     * @secure
     */
    stepRunUpdateRerun: (tenant: string, stepRun: string, data: RerunStepRunRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<StepRun, any, {}>>;
    /**
     * @description Attempts to cancel a step run
     *
     * @tags Step Run
     * @name StepRunUpdateCancel
     * @summary Attempts to cancel a step run
     * @request POST:/api/v1/tenants/{tenant}/step-runs/{step-run}/cancel
     * @secure
     */
    stepRunUpdateCancel: (tenant: string, stepRun: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<StepRun, any, {}>>;
    /**
     * @description Get the schema for a step run
     *
     * @tags Step Run
     * @name StepRunGetSchema
     * @summary Get step run schema
     * @request GET:/api/v1/tenants/{tenant}/step-runs/{step-run}/schema
     * @secure
     */
    stepRunGetSchema: (tenant: string, stepRun: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<object, any, {}>>;
    /**
     * @description Get all workers for a tenant
     *
     * @tags Worker
     * @name WorkerList
     * @summary Get workers
     * @request GET:/api/v1/tenants/{tenant}/worker
     * @secure
     */
    workerList: (tenant: string, query?: {
        /**
         * The number to skip
         * @format int64
         */
        offset?: number;
        /**
         * The number to limit by
         * @format int64
         */
        limit?: number;
        /** Filter by worker status */
        statuses?: WorkerStatus[];
        /** Filter by worker labels */
        labels?: string[];
        /** Whether to include engine-managed operator workers, which are hidden by default */
        includeOperators?: boolean;
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<WorkerList, any, {}>>;
    /**
     * @description Update a worker
     *
     * @tags Worker
     * @name WorkerUpdate
     * @summary Update worker
     * @request PATCH:/api/v1/workers/{worker}
     * @secure
     */
    workerUpdate: (worker: string, data: UpdateWorkerRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<Worker, any, {}>>;
    /**
     * @description Get a worker
     *
     * @tags Worker
     * @name WorkerGet
     * @summary Get worker
     * @request GET:/api/v1/workers/{worker}
     * @secure
     */
    workerGet: (worker: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<Worker, any, {}>>;
    /**
     * @description Lists all webhooks
     *
     * @name WebhookList
     * @summary List webhooks
     * @request GET:/api/v1/tenants/{tenant}/webhook-workers
     * @secure
     */
    webhookList: (tenant: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<WebhookWorkerListResponse, any, {}>>;
    /**
     * @description Creates a webhook
     *
     * @name WebhookCreate
     * @summary Create a webhook
     * @request POST:/api/v1/tenants/{tenant}/webhook-workers
     * @secure
     */
    webhookCreate: (tenant: string, data: WebhookWorkerCreateRequest, params?: RequestParams) => Promise<import("axios").AxiosResponse<WebhookWorkerCreated, any, {}>>;
    /**
     * @description Deletes a webhook
     *
     * @name WebhookDelete
     * @summary Delete a webhook
     * @request DELETE:/api/v1/webhook-workers/{webhook}
     * @secure
     */
    webhookDelete: (webhook: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<void, any, {}>>;
    /**
     * @description Lists all requests for a webhook
     *
     * @name WebhookRequestsList
     * @summary List webhook requests
     * @request GET:/api/v1/webhook-workers/{webhook}/requests
     * @secure
     */
    webhookRequestsList: (webhook: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<WebhookWorkerRequestListResponse, any, {}>>;
    /**
     * @description Get the input for a workflow run.
     *
     * @tags Workflow Run
     * @name WorkflowRunGetInput
     * @summary Get workflow run input
     * @request GET:/api/v1/tenants/{tenant}/workflow-runs/{workflow-run}/input
     * @secure
     */
    workflowRunGetInput: (tenant: string, workflowRun: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<Record<string, any>, any, {}>>;
    /**
     * @description Triggers a workflow to check the status of the instance
     *
     * @name MonitoringPostRunProbe
     * @summary Detailed Health Probe For the Instance
     * @request POST:/api/v1/monitoring/{tenant}/probe
     * @secure
     */
    monitoringPostRunProbe: (tenant: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<void, any, {}>>;
    /**
     * @description Get the version of the server
     *
     * @name InfoGetVersion
     * @summary We return the version for the currently running server
     * @request GET:/api/v1/version
     */
    infoGetVersion: (params?: RequestParams) => Promise<import("axios").AxiosResponse<{
        /** @example "1.0.0" */
        version: string;
    }, any, {}>>;
    /**
     * @description Get the prometheus metrics for the tenant
     *
     * @tags Tenant
     * @name TenantGetPrometheusMetrics
     * @summary Get prometheus metrics
     * @request GET:/api/v1/tenants/{tenant}/prometheus-metrics
     * @secure
     */
    tenantGetPrometheusMetrics: (tenant: string, params?: RequestParams) => Promise<import("axios").AxiosResponse<string, any, {}>>;
    /**
     * @description Get task stats for tenant
     *
     * @tags Tenant
     * @name TenantGetTaskStats
     * @summary Get task stats for tenant
     * @request GET:/api/v1/tenants/{tenant}/task-stats
     * @secure
     */
    tenantGetTaskStats: (tenant: string, query?: {
        /** Task names that must appear in the response. Missing tasks are zero-filled so KEDA's metrics-api JSONPath always resolves. */
        taskNames?: string[];
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<TaskStats, any, {}>>;
    /**
     * @description Evaluate a feature flag for a tenant
     *
     * @tags Feature Flags
     * @name TenantFeatureFlagEvaluate
     * @summary Evaluate a feature flag for a tenant
     * @request GET:/api/v1/tenants/{tenant}/feature-flags
     * @secure
     */
    tenantFeatureFlagEvaluate: (tenant: string, query: {
        /** The feature flag id to evaluate */
        featureFlagId: FeatureFlagId;
        /** A flag indicating what the behavior of the feature flag should be if PostHog is disabled or unavailable */
        isEnabledIfNoPosthog: boolean;
    }, params?: RequestParams) => Promise<import("axios").AxiosResponse<FeatureFlagEvaluationResult, any, {}>>;
}
