export declare const ONBOARDING_EVENT_KEY = "user:onboarding-completed";
export type SignupInput = {
    email: string;
    user_id: string;
};
export type WelcomeEmailResult = {
    userId: string;
    welcomeSent: boolean;
    followUpSent: boolean;
};
export declare const welcomeEmail: import("../..").TaskWorkflowDeclaration<SignupInput, WelcomeEmailResult, {}, {}, {}, {}>;
