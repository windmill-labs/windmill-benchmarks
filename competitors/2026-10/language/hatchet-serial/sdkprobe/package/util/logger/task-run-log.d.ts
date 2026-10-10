import { ActionType } from '../../protoc/dispatcher';
export declare function actionMap(action: ActionType): string;
export declare function taskRunLog(taskName: string, taskRunExternalId: string, action: string): string;
