export declare const childIndexChild: import("../..").TaskWorkflowDeclaration<{
    tag: string;
}, {
    tag: string;
}, {}, {}, {}, {}>;
export declare const childIndexParent: import("../..").WorkflowDeclaration<{
    n: number;
}, {}, {}>;
export declare const scenarioTask: import("../..").TaskWorkflowDeclaration<{
    scenarioId: string;
}, {
    scenarioId: string;
    status: string;
}, {}, {}, {}, {}>;
export declare const orchestratorTask: import("../..").TaskWorkflowDeclaration<import("../..").UnknownInputType, {
    total: number;
    expected: number;
    uniqueRunIds: number;
    hasDuplicates: boolean;
    scenarios: string[];
}, {}, {}, {}, {}>;
