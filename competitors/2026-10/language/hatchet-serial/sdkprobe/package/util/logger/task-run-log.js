"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.actionMap = actionMap;
exports.taskRunLog = taskRunLog;
const dispatcher_1 = require("../../protoc/dispatcher");
function actionMap(action) {
    switch (action) {
        case dispatcher_1.ActionType.START_STEP_RUN:
            return 'starting...';
        case dispatcher_1.ActionType.CANCEL_STEP_RUN:
            return 'cancelling...';
        case dispatcher_1.ActionType.START_GET_GROUP_KEY:
            return 'starting to get group key...';
        default:
            return 'unknown';
    }
}
function taskRunLog(taskName, taskRunExternalId, action) {
    return `Task run ${action} \t ${taskName}/${taskRunExternalId} `;
}
