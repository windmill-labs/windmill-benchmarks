import { Priority } from '../..';
export declare const priority: import("../..").TaskWorkflowDeclaration<import("../..").UnknownInputType, {
    priority: Priority | undefined;
}, {}, {}, {}, {}>;
export declare const priorityWf: import("../..").WorkflowDeclaration<import("../..").UnknownInputType, {}, {}>;
export declare const priorityTasks: (import("../..").WorkflowDeclaration<import("../..").UnknownInputType, {}, {}> | import("../..").TaskWorkflowDeclaration<import("../..").UnknownInputType, {
    priority: Priority | undefined;
}, {}, {}, {}, {}>)[];
