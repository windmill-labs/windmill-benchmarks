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
exports.Api = void 0;
const http_client_1 = require("./http-client");
class Api extends http_client_1.HttpClient {
    constructor() {
        super(...arguments);
        /**
         * @description Get a task by id
         *
         * @tags Task
         * @name V1TaskGet
         * @summary Get a task
         * @request GET:/api/v1/stable/tasks/{task}
         * @secure
         */
        this.v1TaskGet = (task, query, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tasks/${task}`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description List events for a task
         *
         * @tags Task
         * @name V1TaskEventList
         * @summary List events for a task
         * @request GET:/api/v1/stable/tasks/{task}/task-events
         * @secure
         */
        this.v1TaskEventList = (task, query, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tasks/${task}/task-events`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description Lists log lines for a task
         *
         * @tags Log
         * @name V1LogLineList
         * @summary List log lines
         * @request GET:/api/v1/stable/tasks/{task}/logs
         * @secure
         */
        this.v1LogLineList = (task, query, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tasks/${task}/logs`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description Cancel tasks
         *
         * @tags Task
         * @name V1TaskCancel
         * @summary Cancel tasks
         * @request POST:/api/v1/stable/tenants/{tenant}/tasks/cancel
         * @secure
         */
        this.v1TaskCancel = (tenant, data, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tenants/${tenant}/tasks/cancel`, method: 'POST', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Lists log lines for a tenant
         *
         * @tags Log
         * @name V1TenantLogLineList
         * @summary List log lines
         * @request GET:/api/v1/stable/tenants/{tenant}/logs
         * @secure
         */
        this.v1TenantLogLineList = (tenant, query, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tenants/${tenant}/logs`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description Get a minute by minute breakdown of log metrics for a tenant
         *
         * @tags Log
         * @name V1TenantLogLineGetPointMetrics
         * @summary Get log point metrics
         * @request GET:/api/v1/stable/tenants/{tenant}/log-point-metrics
         * @secure
         */
        this.v1TenantLogLineGetPointMetrics = (tenant, query, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tenants/${tenant}/log-point-metrics`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description Replay tasks
         *
         * @tags Task
         * @name V1TaskReplay
         * @summary Replay tasks
         * @request POST:/api/v1/stable/tenants/{tenant}/tasks/replay
         * @secure
         */
        this.v1TaskReplay = (tenant, data, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tenants/${tenant}/tasks/replay`, method: 'POST', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Restore an evicted durable task
         *
         * @tags Task
         * @name V1TaskRestore
         * @summary Restore a task
         * @request POST:/api/v1/stable/tasks/{task}/restore
         * @secure
         */
        this.v1TaskRestore = (task, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tasks/${task}/restore`, method: 'POST', secure: true, format: 'json' }, params));
        /**
         * @description Lists all tasks that belong a specific list of dags
         *
         * @tags Task
         * @name V1DagListTasks
         * @summary List tasks
         * @request GET:/api/v1/stable/dags/tasks
         * @secure
         */
        this.v1DagListTasks = (query, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/dags/tasks`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description Lists workflow runs for a tenant.
         *
         * @tags Workflow Runs
         * @name V1WorkflowRunList
         * @summary List workflow runs
         * @request GET:/api/v1/stable/tenants/{tenant}/workflow-runs
         * @secure
         */
        this.v1WorkflowRunList = (tenant, query, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tenants/${tenant}/workflow-runs`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description Lists displayable names of workflow runs for a tenant
         *
         * @tags Workflow Runs
         * @name V1WorkflowRunDisplayNamesList
         * @summary List workflow runs
         * @request GET:/api/v1/stable/tenants/{tenant}/workflow-runs/display-names
         * @secure
         */
        this.v1WorkflowRunDisplayNamesList = (tenant, query, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tenants/${tenant}/workflow-runs/display-names`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description Lists external ids for workflow runs matching filters
         *
         * @tags Workflow Runs
         * @name V1WorkflowRunExternalIdsList
         * @summary List workflow run external ids
         * @request GET:/api/v1/stable/tenants/{tenant}/workflow-runs/external-ids
         * @secure
         */
        this.v1WorkflowRunExternalIdsList = (tenant, query, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tenants/${tenant}/workflow-runs/external-ids`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description Trigger a new workflow run
         *
         * @tags Workflow Runs
         * @name V1WorkflowRunCreate
         * @summary Create workflow run
         * @request POST:/api/v1/stable/tenants/{tenant}/workflow-runs/trigger
         * @secure
         */
        this.v1WorkflowRunCreate = (tenant, data, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tenants/${tenant}/workflow-runs/trigger`, method: 'POST', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Branch a durable task from a specific node, creating a new branch and re-processing its matches.
         *
         * @tags Workflow Runs
         * @name V1DurableTaskBranch
         * @summary Branch durable task
         * @request POST:/api/v1/stable/tenants/{tenant}/durable-tasks/branch
         * @secure
         */
        this.v1DurableTaskBranch = (tenant, data, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tenants/${tenant}/durable-tasks/branch`, method: 'POST', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Lists all event log entries for a durable task.
         *
         * @tags Durable Tasks
         * @name V1DurableTaskEventLogList
         * @summary List durable event log
         * @request GET:/api/v1/stable/tenants/{tenant}/durable-tasks/{durable-task}
         * @secure
         */
        this.v1DurableTaskEventLogList = (tenant, durableTask, query, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tenants/${tenant}/durable-tasks/${durableTask}`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description Get a durable stream payload uploaded ahead of its publish, by the payload_ref of the message that references it.
         *
         * @tags Streams
         * @name V1StreamPayloadGet
         * @summary Get stream payload
         * @request GET:/api/v1/stable/tenants/{tenant}/streams/payloads
         * @secure
         */
        this.v1StreamPayloadGet = (tenant, query, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tenants/${tenant}/streams/payloads`, method: 'GET', query: query, secure: true }, params));
        /**
         * @description Upload a payload too large for a gRPC publish, then publish the message with the returned ref as its payload_ref.
         *
         * @tags Streams
         * @name V1StreamPayloadUpload
         * @summary Upload stream payload
         * @request POST:/api/v1/stable/tenants/{tenant}/streams/payloads
         * @secure
         */
        this.v1StreamPayloadUpload = (tenant, data, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tenants/${tenant}/streams/payloads`, method: 'POST', body: data, secure: true, type: http_client_1.ContentType.FormData, format: 'json' }, params));
        /**
         * @description Get a workflow run and its metadata to display on the "detail" page
         *
         * @tags Workflow Runs
         * @name V1WorkflowRunGet
         * @summary List tasks
         * @request GET:/api/v1/stable/workflow-runs/{v1-workflow-run}
         * @secure
         */
        this.v1WorkflowRunGet = (v1WorkflowRun, query, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/workflow-runs/${v1WorkflowRun}`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description Get the status of a workflow run.
         *
         * @tags Workflow Runs
         * @name V1WorkflowRunGetStatus
         * @summary Get workflow run status
         * @request GET:/api/v1/stable/workflow-runs/{v1-workflow-run}/status
         * @secure
         */
        this.v1WorkflowRunGetStatus = (v1WorkflowRun, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/workflow-runs/${v1WorkflowRun}/status`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description List all tasks for a workflow run
         *
         * @tags Workflow Runs
         * @name V1WorkflowRunTaskEventsList
         * @summary List tasks
         * @request GET:/api/v1/stable/workflow-runs/{v1-workflow-run}/task-events
         * @secure
         */
        this.v1WorkflowRunTaskEventsList = (v1WorkflowRun, query, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/workflow-runs/${v1WorkflowRun}/task-events`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description Get OTel trace for a workflow run
         *
         * @tags Observability
         * @name V1ObservabilityGetTrace
         * @summary Get OTel trace
         * @request GET:/api/v1/stable/tenants/{tenant}/traces
         * @secure
         */
        this.v1ObservabilityGetTrace = (tenant, query, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tenants/${tenant}/traces`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description Get the timings for a workflow run
         *
         * @tags Workflow Runs
         * @name V1WorkflowRunGetTimings
         * @summary List timings for a workflow run
         * @request GET:/api/v1/stable/workflow-runs/{v1-workflow-run}/task-timings
         * @secure
         */
        this.v1WorkflowRunGetTimings = (v1WorkflowRun, query, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/workflow-runs/${v1WorkflowRun}/task-timings`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description Get a summary of task run metrics for a tenant
         *
         * @tags Task
         * @name V1TaskListStatusMetrics
         * @summary Get task metrics
         * @request GET:/api/v1/stable/tenants/{tenant}/task-metrics
         * @secure
         */
        this.v1TaskListStatusMetrics = (tenant, query, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tenants/${tenant}/task-metrics`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description Get a minute by minute breakdown of task metrics for a tenant
         *
         * @tags Task
         * @name V1TaskGetPointMetrics
         * @summary Get task point metrics
         * @request GET:/api/v1/stable/tenants/{tenant}/task-point-metrics
         * @secure
         */
        this.v1TaskGetPointMetrics = (tenant, query, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tenants/${tenant}/task-point-metrics`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description Lists all events for a tenant.
         *
         * @tags Event
         * @name V1EventList
         * @summary List events
         * @request GET:/api/v1/stable/tenants/{tenant}/events
         * @secure
         */
        this.v1EventList = (tenant, query, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tenants/${tenant}/events`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description Get an event by its id
         *
         * @tags Event
         * @name V1EventGet
         * @summary Get events
         * @request GET:/api/v1/stable/tenants/{tenant}/events/{v1-event}
         * @secure
         */
        this.v1EventGet = (tenant, v1Event, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tenants/${tenant}/events/${v1Event}`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Lists all event keys for a tenant.
         *
         * @tags Event
         * @name V1EventKeyList
         * @summary List event keys
         * @request GET:/api/v1/stable/tenants/{tenant}/events/keys
         * @secure
         */
        this.v1EventKeyList = (tenant, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tenants/${tenant}/events/keys`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Lists all filters for a tenant.
         *
         * @tags Filter
         * @name V1FilterList
         * @summary List filters
         * @request GET:/api/v1/stable/tenants/{tenant}/filters
         * @secure
         */
        this.v1FilterList = (tenant, query, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tenants/${tenant}/filters`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description Create a new filter
         *
         * @tags Filter
         * @name V1FilterCreate
         * @summary Create a filter
         * @request POST:/api/v1/stable/tenants/{tenant}/filters
         * @secure
         */
        this.v1FilterCreate = (tenant, data, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tenants/${tenant}/filters`, method: 'POST', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Get a filter by its id
         *
         * @tags Filter
         * @name V1FilterGet
         * @summary Get a filter
         * @request GET:/api/v1/stable/tenants/{tenant}/filters/{v1-filter}
         * @secure
         */
        this.v1FilterGet = (tenant, v1Filter, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tenants/${tenant}/filters/${v1Filter}`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Delete a filter
         *
         * @tags Filter
         * @name V1FilterDelete
         * @summary Delete a filter
         * @request DELETE:/api/v1/stable/tenants/{tenant}/filters/{v1-filter}
         * @secure
         */
        this.v1FilterDelete = (tenant, v1Filter, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tenants/${tenant}/filters/${v1Filter}`, method: 'DELETE', secure: true, format: 'json' }, params));
        /**
         * @description Update a filter
         *
         * @tags Filter
         * @name V1FilterUpdate
         * @summary Update a filter
         * @request PATCH:/api/v1/stable/tenants/{tenant}/filters/{v1-filter}
         * @secure
         */
        this.v1FilterUpdate = (tenant, v1Filter, data, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tenants/${tenant}/filters/${v1Filter}`, method: 'PATCH', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Lists all webhook for a tenant.
         *
         * @tags Webhook
         * @name V1WebhookList
         * @summary List webhooks
         * @request GET:/api/v1/stable/tenants/{tenant}/webhooks
         * @secure
         */
        this.v1WebhookList = (tenant, query, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tenants/${tenant}/webhooks`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description Create a new webhook
         *
         * @tags Webhook
         * @name V1WebhookCreate
         * @summary Create a webhook
         * @request POST:/api/v1/stable/tenants/{tenant}/webhooks
         * @secure
         */
        this.v1WebhookCreate = (tenant, data, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tenants/${tenant}/webhooks`, method: 'POST', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Get a webhook by its name
         *
         * @tags Webhook
         * @name V1WebhookGet
         * @summary Get a webhook
         * @request GET:/api/v1/stable/tenants/{tenant}/webhooks/{v1-webhook}
         * @secure
         */
        this.v1WebhookGet = (tenant, v1Webhook, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tenants/${tenant}/webhooks/${v1Webhook}`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Delete a webhook
         *
         * @tags Webhook
         * @name V1WebhookDelete
         * @summary Delete a webhook
         * @request DELETE:/api/v1/stable/tenants/{tenant}/webhooks/{v1-webhook}
         * @secure
         */
        this.v1WebhookDelete = (tenant, v1Webhook, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tenants/${tenant}/webhooks/${v1Webhook}`, method: 'DELETE', secure: true, format: 'json' }, params));
        /**
         * @description Post an incoming webhook message
         *
         * @tags Webhook
         * @name V1WebhookReceive
         * @summary Post a webhook message
         * @request POST:/api/v1/stable/tenants/{tenant}/webhooks/{v1-webhook}
         */
        this.v1WebhookReceive = (tenant, v1Webhook, data, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tenants/${tenant}/webhooks/${v1Webhook}`, method: 'POST', body: data, format: 'json' }, params));
        /**
         * @description Update a webhook
         *
         * @tags Webhook
         * @name V1WebhookUpdate
         * @summary Update a webhook
         * @request PATCH:/api/v1/stable/tenants/{tenant}/webhooks/{v1-webhook}
         * @secure
         */
        this.v1WebhookUpdate = (tenant, v1Webhook, data, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tenants/${tenant}/webhooks/${v1Webhook}`, method: 'PATCH', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Evaluate a CEL expression against provided input data.
         *
         * @tags CEL
         * @name V1CelDebug
         * @summary Debug a CEL expression
         * @request POST:/api/v1/stable/tenants/{tenant}/cel/debug
         * @secure
         */
        this.v1CelDebug = (tenant, data, params = {}) => this.request(Object.assign({ path: `/api/v1/stable/tenants/${tenant}/cel/debug`, method: 'POST', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Gets the readiness status
         *
         * @tags Healthcheck
         * @name ReadinessGet
         * @summary Get readiness
         * @request GET:/api/ready
         */
        this.readinessGet = (params = {}) => this.request(Object.assign({ path: `/api/ready`, method: 'GET' }, params));
        /**
         * @description Gets the liveness status
         *
         * @tags Healthcheck
         * @name LivenessGet
         * @summary Get liveness
         * @request GET:/api/live
         */
        this.livenessGet = (params = {}) => this.request(Object.assign({ path: `/api/live`, method: 'GET' }, params));
        /**
         * @description Gets metadata for the Hatchet instance
         *
         * @tags Metadata
         * @name MetadataGet
         * @summary Get metadata
         * @request GET:/api/v1/meta
         */
        this.metadataGet = (params = {}) => this.request(Object.assign({ path: `/api/v1/meta`, method: 'GET', format: 'json' }, params));
        /**
         * @description Gets metadata for the Hatchet cloud instance
         *
         * @tags Metadata
         * @name CloudMetadataGet
         * @summary Get cloud metadata
         * @request GET:/api/v1/cloud/metadata
         */
        this.cloudMetadataGet = (params = {}) => this.request(Object.assign({ path: `/api/v1/cloud/metadata`, method: 'GET', format: 'json' }, params));
        /**
         * @description List all integrations
         *
         * @tags Metadata
         * @name MetadataListIntegrations
         * @summary List integrations
         * @request GET:/api/v1/meta/integrations
         * @secure
         */
        this.metadataListIntegrations = (params = {}) => this.request(Object.assign({ path: `/api/v1/meta/integrations`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Logs in a user.
         *
         * @tags User
         * @name UserUpdateLogin
         * @summary Login user
         * @request POST:/api/v1/users/login
         */
        this.userUpdateLogin = (data, params = {}) => this.request(Object.assign({ path: `/api/v1/users/login`, method: 'POST', body: data, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Starts the OAuth flow
         *
         * @tags User
         * @name UserUpdateGoogleOauthStart
         * @summary Start OAuth flow
         * @request GET:/api/v1/users/google/start
         */
        this.userUpdateGoogleOauthStart = (params = {}) => this.request(Object.assign({ path: `/api/v1/users/google/start`, method: 'GET' }, params));
        /**
         * @description Completes the OAuth flow
         *
         * @tags User
         * @name UserUpdateGoogleOauthCallback
         * @summary Complete OAuth flow
         * @request GET:/api/v1/users/google/callback
         */
        this.userUpdateGoogleOauthCallback = (params = {}) => this.request(Object.assign({ path: `/api/v1/users/google/callback`, method: 'GET' }, params));
        /**
         * @description Starts the OAuth flow
         *
         * @tags User
         * @name UserUpdateGithubOauthStart
         * @summary Start OAuth flow
         * @request GET:/api/v1/users/github/start
         */
        this.userUpdateGithubOauthStart = (params = {}) => this.request(Object.assign({ path: `/api/v1/users/github/start`, method: 'GET' }, params));
        /**
         * @description Completes the OAuth flow
         *
         * @tags User
         * @name UserUpdateGithubOauthCallback
         * @summary Complete OAuth flow
         * @request GET:/api/v1/users/github/callback
         */
        this.userUpdateGithubOauthCallback = (params = {}) => this.request(Object.assign({ path: `/api/v1/users/github/callback`, method: 'GET' }, params));
        /**
         * @description Starts the OAuth flow
         *
         * @tags User
         * @name UserUpdateSlackOauthStart
         * @summary Start OAuth flow
         * @request GET:/api/v1/tenants/{tenant}/slack/start
         * @secure
         */
        this.userUpdateSlackOauthStart = (tenant, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/slack/start`, method: 'GET', secure: true }, params));
        /**
         * @description Completes the OAuth flow
         *
         * @tags User
         * @name UserUpdateSlackOauthCallback
         * @summary Complete OAuth flow
         * @request GET:/api/v1/users/slack/callback
         * @secure
         */
        this.userUpdateSlackOauthCallback = (params = {}) => this.request(Object.assign({ path: `/api/v1/users/slack/callback`, method: 'GET', secure: true }, params));
        /**
         * @description SNS event
         *
         * @tags Github
         * @name SnsUpdate
         * @summary Github app tenant webhook
         * @request POST:/api/v1/sns/{tenant}/{event}
         */
        this.snsUpdate = (tenant, event, params = {}) => this.request(Object.assign({ path: `/api/v1/sns/${tenant}/${event}`, method: 'POST' }, params));
        /**
         * @description List SNS integrations
         *
         * @tags SNS
         * @name SnsList
         * @summary List SNS integrations
         * @request GET:/api/v1/tenants/{tenant}/sns
         * @secure
         */
        this.snsList = (tenant, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/sns`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Create SNS integration
         *
         * @tags SNS
         * @name SnsCreate
         * @summary Create SNS integration
         * @request POST:/api/v1/tenants/{tenant}/sns
         * @secure
         */
        this.snsCreate = (tenant, data, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/sns`, method: 'POST', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Creates a new tenant alert email group
         *
         * @tags Tenant
         * @name AlertEmailGroupCreate
         * @summary Create tenant alert email group
         * @request POST:/api/v1/tenants/{tenant}/alerting-email-groups
         * @secure
         */
        this.alertEmailGroupCreate = (tenant, data, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/alerting-email-groups`, method: 'POST', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Gets a list of tenant alert email groups
         *
         * @tags Tenant
         * @name AlertEmailGroupList
         * @summary List tenant alert email groups
         * @request GET:/api/v1/tenants/{tenant}/alerting-email-groups
         * @secure
         */
        this.alertEmailGroupList = (tenant, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/alerting-email-groups`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Gets the resource policy for a tenant
         *
         * @tags Tenant
         * @name TenantResourcePolicyGet
         * @summary Create tenant alert email group
         * @request GET:/api/v1/tenants/{tenant}/resource-policy
         * @secure
         */
        this.tenantResourcePolicyGet = (tenant, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/resource-policy`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Updates a tenant alert email group
         *
         * @tags Tenant
         * @name AlertEmailGroupUpdate
         * @summary Update tenant alert email group
         * @request PATCH:/api/v1/alerting-email-groups/{alert-email-group}
         * @secure
         */
        this.alertEmailGroupUpdate = (alertEmailGroup, data, params = {}) => this.request(Object.assign({ path: `/api/v1/alerting-email-groups/${alertEmailGroup}`, method: 'PATCH', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Deletes a tenant alert email group
         *
         * @tags Tenant
         * @name AlertEmailGroupDelete
         * @summary Delete tenant alert email group
         * @request DELETE:/api/v1/alerting-email-groups/{alert-email-group}
         * @secure
         */
        this.alertEmailGroupDelete = (alertEmailGroup, params = {}) => this.request(Object.assign({ path: `/api/v1/alerting-email-groups/${alertEmailGroup}`, method: 'DELETE', secure: true }, params));
        /**
         * @description Delete SNS integration
         *
         * @tags SNS
         * @name SnsDelete
         * @summary Delete SNS integration
         * @request DELETE:/api/v1/sns/{sns}
         * @secure
         */
        this.snsDelete = (sns, params = {}) => this.request(Object.assign({ path: `/api/v1/sns/${sns}`, method: 'DELETE', secure: true }, params));
        /**
         * @description List Slack webhooks
         *
         * @tags Slack
         * @name SlackWebhookList
         * @summary List Slack integrations
         * @request GET:/api/v1/tenants/{tenant}/slack
         * @secure
         */
        this.slackWebhookList = (tenant, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/slack`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Delete Slack webhook
         *
         * @tags Slack
         * @name SlackWebhookDelete
         * @summary Delete Slack webhook
         * @request DELETE:/api/v1/slack/{slack}
         * @secure
         */
        this.slackWebhookDelete = (slack, params = {}) => this.request(Object.assign({ path: `/api/v1/slack/${slack}`, method: 'DELETE', secure: true }, params));
        /**
         * @description Gets the current user
         *
         * @tags User
         * @name UserGetCurrent
         * @summary Get current user
         * @request GET:/api/v1/users/current
         * @secure
         */
        this.userGetCurrent = (params = {}) => this.request(Object.assign({ path: `/api/v1/users/current`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Update a user password.
         *
         * @tags User
         * @name UserUpdatePassword
         * @summary Change user password
         * @request POST:/api/v1/users/password
         * @secure
         */
        this.userUpdatePassword = (data, params = {}) => this.request(Object.assign({ path: `/api/v1/users/password`, method: 'POST', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Registers a user.
         *
         * @tags User
         * @name UserCreate
         * @summary Register user
         * @request POST:/api/v1/users/register
         */
        this.userCreate = (data, params = {}) => this.request(Object.assign({ path: `/api/v1/users/register`, method: 'POST', body: data, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Logs out a user.
         *
         * @tags User
         * @name UserUpdateLogout
         * @summary Logout user
         * @request POST:/api/v1/users/logout
         * @secure
         */
        this.userUpdateLogout = (params = {}) => this.request(Object.assign({ path: `/api/v1/users/logout`, method: 'POST', secure: true, format: 'json' }, params));
        /**
         * @description Lists all tenant memberships for the current user
         *
         * @tags User
         * @name TenantMembershipsList
         * @summary List tenant memberships
         * @request GET:/api/v1/users/memberships
         * @secure
         */
        this.tenantMembershipsList = (params = {}) => this.request(Object.assign({ path: `/api/v1/users/memberships`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Lists all tenant invites for the current user
         *
         * @tags Tenant
         * @name UserListTenantInvites
         * @summary List tenant invites
         * @request GET:/api/v1/users/invites
         * @secure
         */
        this.userListTenantInvites = (params = {}) => this.request(Object.assign({ path: `/api/v1/users/invites`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Accepts a tenant invite
         *
         * @tags Tenant
         * @name TenantInviteAccept
         * @summary Accept tenant invite
         * @request POST:/api/v1/users/invites/accept
         * @secure
         */
        this.tenantInviteAccept = (data, params = {}) => this.request(Object.assign({ path: `/api/v1/users/invites/accept`, method: 'POST', body: data, secure: true, type: http_client_1.ContentType.Json }, params));
        /**
         * @description Rejects a tenant invite
         *
         * @tags Tenant
         * @name TenantInviteReject
         * @summary Reject tenant invite
         * @request POST:/api/v1/users/invites/reject
         * @secure
         */
        this.tenantInviteReject = (data, params = {}) => this.request(Object.assign({ path: `/api/v1/users/invites/reject`, method: 'POST', body: data, secure: true, type: http_client_1.ContentType.Json }, params));
        /**
         * @description Creates a new tenant
         *
         * @tags Tenant
         * @name TenantCreate
         * @summary Create tenant
         * @request POST:/api/v1/tenants
         * @secure
         */
        this.tenantCreate = (data, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants`, method: 'POST', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Update an existing tenant
         *
         * @tags Tenant
         * @name TenantUpdate
         * @summary Update tenant
         * @request PATCH:/api/v1/tenants/{tenant}
         * @secure
         */
        this.tenantUpdate = (tenant, data, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}`, method: 'PATCH', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Get the details of a tenant
         *
         * @tags Tenant
         * @name TenantGet
         * @summary Get tenant
         * @request GET:/api/v1/tenants/{tenant}
         * @secure
         */
        this.tenantGet = (tenant, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Gets the alerting settings for a tenant
         *
         * @tags Tenant
         * @name TenantAlertingSettingsGet
         * @summary Get tenant alerting settings
         * @request GET:/api/v1/tenants/{tenant}/alerting/settings
         * @secure
         */
        this.tenantAlertingSettingsGet = (tenant, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/alerting/settings`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Creates a new tenant invite
         *
         * @tags Tenant
         * @name TenantInviteCreate
         * @summary Create tenant invite
         * @request POST:/api/v1/tenants/{tenant}/invites
         * @secure
         */
        this.tenantInviteCreate = (tenant, data, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/invites`, method: 'POST', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Gets a list of tenant invites
         *
         * @tags Tenant
         * @name TenantInviteList
         * @summary List tenant invites
         * @request GET:/api/v1/tenants/{tenant}/invites
         * @secure
         */
        this.tenantInviteList = (tenant, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/invites`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Updates a tenant invite
         *
         * @name TenantInviteUpdate
         * @summary Update invite
         * @request PATCH:/api/v1/tenants/{tenant}/invites/{tenant-invite}
         * @secure
         */
        this.tenantInviteUpdate = (tenant, tenantInvite, data, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/invites/${tenantInvite}`, method: 'PATCH', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Deletes a tenant invite
         *
         * @name TenantInviteDelete
         * @summary Delete invite
         * @request DELETE:/api/v1/tenants/{tenant}/invites/{tenant-invite}
         * @secure
         */
        this.tenantInviteDelete = (tenant, tenantInvite, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/invites/${tenantInvite}`, method: 'DELETE', secure: true, format: 'json' }, params));
        /**
         * @description Create an API token for a tenant
         *
         * @tags API Token
         * @name ApiTokenCreate
         * @summary Create API Token
         * @request POST:/api/v1/tenants/{tenant}/api-tokens
         * @secure
         */
        this.apiTokenCreate = (tenant, data, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/api-tokens`, method: 'POST', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description List API tokens for a tenant
         *
         * @tags API Token
         * @name ApiTokenList
         * @summary List API Tokens
         * @request GET:/api/v1/tenants/{tenant}/api-tokens
         * @secure
         */
        this.apiTokenList = (tenant, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/api-tokens`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Revoke an API token for a tenant
         *
         * @tags API Token
         * @name ApiTokenUpdateRevoke
         * @summary Revoke API Token
         * @request POST:/api/v1/api-tokens/{api-token}
         * @secure
         */
        this.apiTokenUpdateRevoke = (apiToken, params = {}) => this.request(Object.assign({ path: `/api/v1/api-tokens/${apiToken}`, method: 'POST', secure: true }, params));
        /**
         * @description Get the queue metrics for the tenant
         *
         * @tags Workflow
         * @name TenantGetQueueMetrics
         * @summary Get workflow metrics
         * @request GET:/api/v1/tenants/{tenant}/queue-metrics
         * @secure
         */
        this.tenantGetQueueMetrics = (tenant, query, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/queue-metrics`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description Get the queue metrics for the tenant
         *
         * @tags Tenant
         * @name TenantGetStepRunQueueMetrics
         * @summary Get step run metrics
         * @request GET:/api/v1/tenants/{tenant}/step-run-queue-metrics
         * @secure
         */
        this.tenantGetStepRunQueueMetrics = (tenant, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/step-run-queue-metrics`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Lists all events for a tenant.
         *
         * @tags Event
         * @name EventList
         * @summary List events
         * @request GET:/api/v1/tenants/{tenant}/events
         * @secure
         */
        this.eventList = (tenant, query, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/events`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description Creates a new event.
         *
         * @tags Event
         * @name EventCreate
         * @summary Create event
         * @request POST:/api/v1/tenants/{tenant}/events
         * @secure
         */
        this.eventCreate = (tenant, data, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/events`, method: 'POST', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Bulk creates new events.
         *
         * @tags Event
         * @name EventCreateBulk
         * @summary Bulk Create events
         * @request POST:/api/v1/tenants/{tenant}/events/bulk
         * @secure
         */
        this.eventCreateBulk = (tenant, data, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/events/bulk`, method: 'POST', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Replays a list of events.
         *
         * @tags Event
         * @name EventUpdateReplay
         * @summary Replay events
         * @request POST:/api/v1/tenants/{tenant}/events/replay
         * @secure
         */
        this.eventUpdateReplay = (tenant, data, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/events/replay`, method: 'POST', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Cancels all runs for a list of events.
         *
         * @tags Event
         * @name EventUpdateCancel
         * @summary Replay events
         * @request POST:/api/v1/tenants/{tenant}/events/cancel
         * @secure
         */
        this.eventUpdateCancel = (tenant, data, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/events/cancel`, method: 'POST', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Lists all rate limits for a tenant.
         *
         * @tags Rate Limits
         * @name RateLimitList
         * @summary List rate limits
         * @request GET:/api/v1/tenants/{tenant}/rate-limits
         * @secure
         */
        this.rateLimitList = (tenant, query, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/rate-limits`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description Delete a rate limit for a tenant.
         *
         * @tags Rate Limits
         * @name RateLimitDelete
         * @summary Delete rate limit
         * @request DELETE:/api/v1/tenants/{tenant}/rate-limits
         * @secure
         */
        this.rateLimitDelete = (tenant, query, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/rate-limits`, method: 'DELETE', query: query, secure: true }, params));
        /**
         * @description Gets a list of tenant members
         *
         * @tags Tenant
         * @name TenantMemberList
         * @summary List tenant members
         * @request GET:/api/v1/tenants/{tenant}/members
         * @secure
         */
        this.tenantMemberList = (tenant, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/members`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Update a tenant member
         *
         * @tags Tenant
         * @name TenantMemberUpdate
         * @summary Update a tenant member
         * @request PATCH:/api/v1/tenants/{tenant}/members/{member}
         * @secure
         */
        this.tenantMemberUpdate = (tenant, member, data, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/members/${member}`, method: 'PATCH', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Delete a member from a tenant
         *
         * @tags Tenant
         * @name TenantMemberDelete
         * @summary Delete a tenant member
         * @request DELETE:/api/v1/tenants/{tenant}/members/{member}
         * @secure
         */
        this.tenantMemberDelete = (tenant, member, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/members/${member}`, method: 'DELETE', secure: true, format: 'json' }, params));
        /**
         * @description Get an event.
         *
         * @tags Event
         * @name EventGet
         * @summary Get event data
         * @request GET:/api/v1/events/{event}
         * @secure
         */
        this.eventGet = (event, params = {}) => this.request(Object.assign({ path: `/api/v1/events/${event}`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Get the data for an event.
         *
         * @tags Event
         * @name EventDataGet
         * @summary Get event data
         * @request GET:/api/v1/events/{event}/data
         * @secure
         */
        this.eventDataGet = (event, params = {}) => this.request(Object.assign({ path: `/api/v1/events/${event}/data`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Get the data for an event.
         *
         * @tags Event
         * @name EventDataGetWithTenant
         * @summary Get event data
         * @request GET:/api/v1/tenants/{tenant}/events/{event-with-tenant}/data
         * @secure
         */
        this.eventDataGetWithTenant = (eventWithTenant, tenant, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/events/${eventWithTenant}/data`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Lists all event keys for a tenant.
         *
         * @tags Event
         * @name EventKeyList
         * @summary List event keys
         * @request GET:/api/v1/tenants/{tenant}/events/keys
         * @secure
         */
        this.eventKeyList = (tenant, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/events/keys`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Get all workflows for a tenant
         *
         * @tags Workflow
         * @name WorkflowList
         * @summary Get workflows
         * @request GET:/api/v1/tenants/{tenant}/workflows
         * @secure
         */
        this.workflowList = (tenant, query, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/workflows`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description Schedule a new workflow run for a tenant
         *
         * @tags Workflow Run
         * @name ScheduledWorkflowRunCreate
         * @summary Trigger workflow run
         * @request POST:/api/v1/tenants/{tenant}/workflows/{workflow}/scheduled
         * @secure
         */
        this.scheduledWorkflowRunCreate = (tenant, workflow, data, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/workflows/${workflow}/scheduled`, method: 'POST', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Get all scheduled workflow runs for a tenant
         *
         * @tags Workflow
         * @name WorkflowScheduledList
         * @summary Get scheduled workflow runs
         * @request GET:/api/v1/tenants/{tenant}/workflows/scheduled
         * @secure
         */
        this.workflowScheduledList = (tenant, query, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/workflows/scheduled`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description Get a scheduled workflow run for a tenant
         *
         * @tags Workflow
         * @name WorkflowScheduledGet
         * @summary Get scheduled workflow run
         * @request GET:/api/v1/tenants/{tenant}/workflows/scheduled/{scheduled-workflow-run}
         * @secure
         */
        this.workflowScheduledGet = (tenant, scheduledWorkflowRun, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/workflows/scheduled/${scheduledWorkflowRun}`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Delete a scheduled workflow run for a tenant
         *
         * @tags Workflow
         * @name WorkflowScheduledDelete
         * @summary Delete scheduled workflow run
         * @request DELETE:/api/v1/tenants/{tenant}/workflows/scheduled/{scheduled-workflow-run}
         * @secure
         */
        this.workflowScheduledDelete = (tenant, scheduledWorkflowRun, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/workflows/scheduled/${scheduledWorkflowRun}`, method: 'DELETE', secure: true }, params));
        /**
         * @description Update (reschedule) a scheduled workflow run for a tenant
         *
         * @tags Workflow
         * @name WorkflowScheduledUpdate
         * @summary Update scheduled workflow run
         * @request PATCH:/api/v1/tenants/{tenant}/workflows/scheduled/{scheduled-workflow-run}
         * @secure
         */
        this.workflowScheduledUpdate = (tenant, scheduledWorkflowRun, data, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/workflows/scheduled/${scheduledWorkflowRun}`, method: 'PATCH', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Trigger a scheduled workflow run immediately for a tenant
         *
         * @tags Workflow
         * @name WorkflowScheduledTrigger
         * @summary Trigger scheduled workflow run
         * @request POST:/api/v1/tenants/{tenant}/workflows/scheduled/{scheduled-workflow-run}
         * @secure
         */
        this.workflowScheduledTrigger = (tenant, scheduledWorkflowRun, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/workflows/scheduled/${scheduledWorkflowRun}`, method: 'POST', secure: true, format: 'json' }, params));
        /**
         * @description Bulk delete scheduled workflow runs for a tenant
         *
         * @tags Workflow
         * @name WorkflowScheduledBulkDelete
         * @summary Bulk delete scheduled workflow runs
         * @request POST:/api/v1/tenants/{tenant}/workflows/scheduled/bulk-delete
         * @secure
         */
        this.workflowScheduledBulkDelete = (tenant, data, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/workflows/scheduled/bulk-delete`, method: 'POST', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Bulk update (reschedule) scheduled workflow runs for a tenant
         *
         * @tags Workflow
         * @name WorkflowScheduledBulkUpdate
         * @summary Bulk update scheduled workflow runs
         * @request POST:/api/v1/tenants/{tenant}/workflows/scheduled/bulk-update
         * @secure
         */
        this.workflowScheduledBulkUpdate = (tenant, data, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/workflows/scheduled/bulk-update`, method: 'POST', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Create a new cron job workflow trigger for a tenant
         *
         * @tags Workflow Run
         * @name CronWorkflowTriggerCreate
         * @summary Create cron job workflow trigger
         * @request POST:/api/v1/tenants/{tenant}/workflows/{workflow}/crons
         * @secure
         */
        this.cronWorkflowTriggerCreate = (tenant, workflow, data, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/workflows/${workflow}/crons`, method: 'POST', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Get all cron job workflow triggers for a tenant
         *
         * @tags Workflow
         * @name CronWorkflowList
         * @summary Get cron job workflows
         * @request GET:/api/v1/tenants/{tenant}/workflows/crons
         * @secure
         */
        this.cronWorkflowList = (tenant, query, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/workflows/crons`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description Get a cron job workflow run for a tenant
         *
         * @tags Workflow
         * @name WorkflowCronGet
         * @summary Get cron job workflow run
         * @request GET:/api/v1/tenants/{tenant}/workflows/crons/{cron-workflow}
         * @secure
         */
        this.workflowCronGet = (tenant, cronWorkflow, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/workflows/crons/${cronWorkflow}`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Delete a cron job workflow run for a tenant
         *
         * @tags Workflow
         * @name WorkflowCronDelete
         * @summary Delete cron job workflow run
         * @request DELETE:/api/v1/tenants/{tenant}/workflows/crons/{cron-workflow}
         * @secure
         */
        this.workflowCronDelete = (tenant, cronWorkflow, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/workflows/crons/${cronWorkflow}`, method: 'DELETE', secure: true }, params));
        /**
         * @description Update a cron workflow for a tenant
         *
         * @tags Workflow
         * @name WorkflowCronUpdate
         * @summary Update cron job workflow run
         * @request PATCH:/api/v1/tenants/{tenant}/workflows/crons/{cron-workflow}
         * @secure
         */
        this.workflowCronUpdate = (tenant, cronWorkflow, data, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/workflows/crons/${cronWorkflow}`, method: 'PATCH', body: data, secure: true, type: http_client_1.ContentType.Json }, params));
        /**
         * @description Trigger a cron workflow immediately for a tenant
         *
         * @tags Workflow
         * @name WorkflowCronTrigger
         * @summary Trigger cron job workflow run immediately
         * @request POST:/api/v1/tenants/{tenant}/workflows/crons/{cron-workflow}
         * @secure
         */
        this.workflowCronTrigger = (tenant, cronWorkflow, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/workflows/crons/${cronWorkflow}`, method: 'POST', secure: true, format: 'json' }, params));
        /**
         * @description Cancel a batch of workflow runs
         *
         * @tags Workflow Run
         * @name WorkflowRunCancel
         * @summary Cancel workflow runs
         * @request POST:/api/v1/tenants/{tenant}/workflows/cancel
         * @secure
         */
        this.workflowRunCancel = (tenant, data, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/workflows/cancel`, method: 'POST', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Get a workflow for a tenant
         *
         * @tags Workflow
         * @name WorkflowGet
         * @summary Get workflow
         * @request GET:/api/v1/workflows/{workflow}
         * @secure
         */
        this.workflowGet = (workflow, params = {}) => this.request(Object.assign({ path: `/api/v1/workflows/${workflow}`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Delete a workflow for a tenant
         *
         * @tags Workflow
         * @name WorkflowDelete
         * @summary Delete workflow
         * @request DELETE:/api/v1/workflows/{workflow}
         * @secure
         */
        this.workflowDelete = (workflow, params = {}) => this.request(Object.assign({ path: `/api/v1/workflows/${workflow}`, method: 'DELETE', secure: true }, params));
        /**
         * @description Update a workflow for a tenant
         *
         * @tags Workflow
         * @name WorkflowUpdate
         * @summary Update workflow
         * @request PATCH:/api/v1/workflows/{workflow}
         * @secure
         */
        this.workflowUpdate = (workflow, data, params = {}) => this.request(Object.assign({ path: `/api/v1/workflows/${workflow}`, method: 'PATCH', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Get a workflow version for a tenant
         *
         * @tags Workflow
         * @name WorkflowVersionGet
         * @summary Get workflow version
         * @request GET:/api/v1/workflows/{workflow}/versions
         * @secure
         */
        this.workflowVersionGet = (workflow, query, params = {}) => this.request(Object.assign({ path: `/api/v1/workflows/${workflow}/versions`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description Trigger a new workflow run for a tenant
         *
         * @tags Workflow Run
         * @name WorkflowRunCreate
         * @summary Trigger workflow run
         * @request POST:/api/v1/workflows/{workflow}/trigger
         * @secure
         */
        this.workflowRunCreate = (workflow, data, query, params = {}) => this.request(Object.assign({ path: `/api/v1/workflows/${workflow}/trigger`, method: 'POST', query: query, body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Get the metrics for a workflow version
         *
         * @tags Workflow
         * @name WorkflowGetMetrics
         * @summary Get workflow metrics
         * @request GET:/api/v1/workflows/{workflow}/metrics
         * @secure
         */
        this.workflowGetMetrics = (workflow, query, params = {}) => this.request(Object.assign({ path: `/api/v1/workflows/${workflow}/metrics`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description List events for a step run
         *
         * @tags Step Run
         * @name StepRunListEvents
         * @summary List events for step run
         * @request GET:/api/v1/step-runs/{step-run}/events
         * @secure
         */
        this.stepRunListEvents = (stepRun, query, params = {}) => this.request(Object.assign({ path: `/api/v1/step-runs/${stepRun}/events`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description List events for all step runs for a workflow run
         *
         * @tags Step Run
         * @name WorkflowRunListStepRunEvents
         * @summary List events for all step runs for a workflow run
         * @request GET:/api/v1/tenants/{tenant}/workflow-runs/{workflow-run}/step-run-events
         * @secure
         */
        this.workflowRunListStepRunEvents = (tenant, workflowRun, query, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/workflow-runs/${workflowRun}/step-run-events`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description List archives for a step run
         *
         * @tags Step Run
         * @name StepRunListArchives
         * @summary List archives for step run
         * @request GET:/api/v1/step-runs/{step-run}/archives
         * @secure
         */
        this.stepRunListArchives = (stepRun, query, params = {}) => this.request(Object.assign({ path: `/api/v1/step-runs/${stepRun}/archives`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description Get a count of the workers available for workflow
         *
         * @tags Workflow
         * @name WorkflowGetWorkersCount
         * @summary Get workflow worker count
         * @request GET:/api/v1/tenants/{tenant}/workflows/{workflow}/worker-count
         * @secure
         */
        this.workflowGetWorkersCount = (tenant, workflow, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/workflows/${workflow}/worker-count`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Get all workflow runs for a tenant
         *
         * @tags Workflow
         * @name WorkflowRunList
         * @summary Get workflow runs
         * @request GET:/api/v1/tenants/{tenant}/workflows/runs
         * @secure
         */
        this.workflowRunList = (tenant, query, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/workflows/runs`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description Replays a list of workflow runs.
         *
         * @tags Workflow Run
         * @name WorkflowRunUpdateReplay
         * @summary Replay workflow runs
         * @request POST:/api/v1/tenants/{tenant}/workflow-runs/replay
         * @secure
         */
        this.workflowRunUpdateReplay = (tenant, data, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/workflow-runs/replay`, method: 'POST', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Get a summary of  workflow run metrics for a tenant
         *
         * @tags Workflow
         * @name WorkflowRunGetMetrics
         * @summary Get workflow runs metrics
         * @request GET:/api/v1/tenants/{tenant}/workflows/runs/metrics
         * @secure
         */
        this.workflowRunGetMetrics = (tenant, query, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/workflows/runs/metrics`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description Get a workflow run for a tenant
         *
         * @tags Workflow
         * @name WorkflowRunGet
         * @summary Get workflow run
         * @request GET:/api/v1/tenants/{tenant}/workflow-runs/{workflow-run}
         * @secure
         */
        this.workflowRunGet = (tenant, workflowRun, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/workflow-runs/${workflowRun}`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Get a workflow run for a tenant
         *
         * @tags Workflow
         * @name WorkflowRunGetShape
         * @summary Get workflow run
         * @request GET:/api/v1/tenants/{tenant}/workflow-runs/{workflow-run}/shape
         * @secure
         */
        this.workflowRunGetShape = (tenant, workflowRun, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/workflow-runs/${workflowRun}/shape`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Get a step run by id
         *
         * @tags Step Run
         * @name StepRunGet
         * @summary Get step run
         * @request GET:/api/v1/tenants/{tenant}/step-runs/{step-run}
         * @secure
         */
        this.stepRunGet = (tenant, stepRun, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/step-runs/${stepRun}`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Reruns a step run
         *
         * @tags Step Run
         * @name StepRunUpdateRerun
         * @summary Rerun step run
         * @request POST:/api/v1/tenants/{tenant}/step-runs/{step-run}/rerun
         * @secure
         */
        this.stepRunUpdateRerun = (tenant, stepRun, data, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/step-runs/${stepRun}/rerun`, method: 'POST', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Attempts to cancel a step run
         *
         * @tags Step Run
         * @name StepRunUpdateCancel
         * @summary Attempts to cancel a step run
         * @request POST:/api/v1/tenants/{tenant}/step-runs/{step-run}/cancel
         * @secure
         */
        this.stepRunUpdateCancel = (tenant, stepRun, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/step-runs/${stepRun}/cancel`, method: 'POST', secure: true, format: 'json' }, params));
        /**
         * @description Get the schema for a step run
         *
         * @tags Step Run
         * @name StepRunGetSchema
         * @summary Get step run schema
         * @request GET:/api/v1/tenants/{tenant}/step-runs/{step-run}/schema
         * @secure
         */
        this.stepRunGetSchema = (tenant, stepRun, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/step-runs/${stepRun}/schema`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Get all workers for a tenant
         *
         * @tags Worker
         * @name WorkerList
         * @summary Get workers
         * @request GET:/api/v1/tenants/{tenant}/worker
         * @secure
         */
        this.workerList = (tenant, query, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/worker`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description Update a worker
         *
         * @tags Worker
         * @name WorkerUpdate
         * @summary Update worker
         * @request PATCH:/api/v1/workers/{worker}
         * @secure
         */
        this.workerUpdate = (worker, data, params = {}) => this.request(Object.assign({ path: `/api/v1/workers/${worker}`, method: 'PATCH', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Get a worker
         *
         * @tags Worker
         * @name WorkerGet
         * @summary Get worker
         * @request GET:/api/v1/workers/{worker}
         * @secure
         */
        this.workerGet = (worker, params = {}) => this.request(Object.assign({ path: `/api/v1/workers/${worker}`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Lists all webhooks
         *
         * @name WebhookList
         * @summary List webhooks
         * @request GET:/api/v1/tenants/{tenant}/webhook-workers
         * @secure
         */
        this.webhookList = (tenant, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/webhook-workers`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Creates a webhook
         *
         * @name WebhookCreate
         * @summary Create a webhook
         * @request POST:/api/v1/tenants/{tenant}/webhook-workers
         * @secure
         */
        this.webhookCreate = (tenant, data, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/webhook-workers`, method: 'POST', body: data, secure: true, type: http_client_1.ContentType.Json, format: 'json' }, params));
        /**
         * @description Deletes a webhook
         *
         * @name WebhookDelete
         * @summary Delete a webhook
         * @request DELETE:/api/v1/webhook-workers/{webhook}
         * @secure
         */
        this.webhookDelete = (webhook, params = {}) => this.request(Object.assign({ path: `/api/v1/webhook-workers/${webhook}`, method: 'DELETE', secure: true }, params));
        /**
         * @description Lists all requests for a webhook
         *
         * @name WebhookRequestsList
         * @summary List webhook requests
         * @request GET:/api/v1/webhook-workers/{webhook}/requests
         * @secure
         */
        this.webhookRequestsList = (webhook, params = {}) => this.request(Object.assign({ path: `/api/v1/webhook-workers/${webhook}/requests`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Get the input for a workflow run.
         *
         * @tags Workflow Run
         * @name WorkflowRunGetInput
         * @summary Get workflow run input
         * @request GET:/api/v1/tenants/{tenant}/workflow-runs/{workflow-run}/input
         * @secure
         */
        this.workflowRunGetInput = (tenant, workflowRun, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/workflow-runs/${workflowRun}/input`, method: 'GET', secure: true, format: 'json' }, params));
        /**
         * @description Triggers a workflow to check the status of the instance
         *
         * @name MonitoringPostRunProbe
         * @summary Detailed Health Probe For the Instance
         * @request POST:/api/v1/monitoring/{tenant}/probe
         * @secure
         */
        this.monitoringPostRunProbe = (tenant, params = {}) => this.request(Object.assign({ path: `/api/v1/monitoring/${tenant}/probe`, method: 'POST', secure: true }, params));
        /**
         * @description Get the version of the server
         *
         * @name InfoGetVersion
         * @summary We return the version for the currently running server
         * @request GET:/api/v1/version
         */
        this.infoGetVersion = (params = {}) => this.request(Object.assign({ path: `/api/v1/version`, method: 'GET', format: 'json' }, params));
        /**
         * @description Get the prometheus metrics for the tenant
         *
         * @tags Tenant
         * @name TenantGetPrometheusMetrics
         * @summary Get prometheus metrics
         * @request GET:/api/v1/tenants/{tenant}/prometheus-metrics
         * @secure
         */
        this.tenantGetPrometheusMetrics = (tenant, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/prometheus-metrics`, method: 'GET', secure: true }, params));
        /**
         * @description Get task stats for tenant
         *
         * @tags Tenant
         * @name TenantGetTaskStats
         * @summary Get task stats for tenant
         * @request GET:/api/v1/tenants/{tenant}/task-stats
         * @secure
         */
        this.tenantGetTaskStats = (tenant, query, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/task-stats`, method: 'GET', query: query, secure: true, format: 'json' }, params));
        /**
         * @description Evaluate a feature flag for a tenant
         *
         * @tags Feature Flags
         * @name TenantFeatureFlagEvaluate
         * @summary Evaluate a feature flag for a tenant
         * @request GET:/api/v1/tenants/{tenant}/feature-flags
         * @secure
         */
        this.tenantFeatureFlagEvaluate = (tenant, query, params = {}) => this.request(Object.assign({ path: `/api/v1/tenants/${tenant}/feature-flags`, method: 'GET', query: query, secure: true, format: 'json' }, params));
    }
}
exports.Api = Api;
