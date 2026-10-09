export declare const REPLY_EVENT_KEY = "support:customer-reply";
export declare const TIMEOUT_SECONDS = 5;
export type SupportTicketInput = {
    ticketId: string;
    customerEmail: string;
    subject: string;
    body: string;
};
export type TriageOutput = {
    category: string;
    priority: string;
};
export type ReplyOutput = {
    message: string;
};
export type EscalationOutput = {
    reason: string;
    assignedTo: string;
};
export declare const triageTicket: import("../..").TaskWorkflowDeclaration<SupportTicketInput, {
    category: string;
    priority: string;
}, {}, {}, {}, {}>;
export declare const generateReply: import("../..").TaskWorkflowDeclaration<SupportTicketInput, {
    message: any;
}, {}, {}, {}, {}>;
export declare const escalateTicket: import("../..").TaskWorkflowDeclaration<SupportTicketInput, {
    reason: string;
    assignedTo: string;
}, {}, {}, {}, {}>;
export declare const supportAgent: import("../..").TaskWorkflowDeclaration<SupportTicketInput, {
    ticketId: string;
    status: "escalated";
    triageCategory: string;
    triagePriority: string;
    initialReply: any;
} | {
    ticketId: string;
    status: "resolved";
    triageCategory: string;
    triagePriority: string;
    initialReply: any;
}, {}, {}, {}, {}>;
