export type WebhookInput = {
    type: string;
    message: string;
};
export declare const webhookWorkflow: import("../..").WorkflowDeclaration<WebhookInput, {}, {}>;
type StripePaymentInput = {
    type: string;
    data: {
        object: {
            customer: string;
            amount: number;
        };
    };
};
export declare const handleStripePayment: import("../..").TaskWorkflowDeclaration<StripePaymentInput, {
    customer: string;
    amount: number;
}, {}, {}, {}, {}>;
type GitHubPRInput = {
    action: string;
    pull_request: {
        number: number;
        title: string;
    };
    repository: {
        full_name: string;
    };
};
export declare const handleGitHubPR: import("../..").TaskWorkflowDeclaration<GitHubPRInput, {
    repo: string;
    pr: number;
}, {}, {}, {}, {}>;
type SlackEventInput = {
    event: {
        type: string;
        user: string;
        text: string;
        channel: string;
    };
};
export declare const handleSlackMention: import("../..").TaskWorkflowDeclaration<SlackEventInput, {
    handled: true;
}, {}, {}, {}, {}>;
type SlackCommandInput = {
    command: string;
    text: string;
    user_name: string;
    response_url: string;
};
export declare const handleSlackCommand: import("../..").TaskWorkflowDeclaration<SlackCommandInput, {
    command: string;
    args: string;
}, {}, {}, {}, {}>;
type SlackInteractionInput = {
    type: string;
    actions: Array<{
        action_id: string;
    }>;
    user: {
        username: string;
    };
};
export declare const handleSlackInteraction: import("../..").TaskWorkflowDeclaration<SlackInteractionInput, {
    action: string;
}, {}, {}, {}, {}>;
export {};
