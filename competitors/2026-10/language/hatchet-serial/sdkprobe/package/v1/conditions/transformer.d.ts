import { TaskConditions } from '../../protoc/v1/shared/condition';
import { CreateWorkflowTaskOpts } from '../task';
import { Condition } from './base';
export declare function taskConditionsToPb(task: Omit<CreateWorkflowTaskOpts<any, any>, 'fn'>, namespace?: string): TaskConditions;
export declare function conditionsToPb(conditions: Condition[], namespace?: string): TaskConditions;
