import { HatchetClient } from '../..';
export type GlobalInputType = {
    first: number;
    second: number;
};
export type GlobalOutputType = {
    extra: number;
};
export declare const hatchetWithMiddleware: HatchetClient<GlobalInputType, GlobalOutputType, {
    dependency: string;
    first: number;
    second: number;
}, {
    additionalData: number;
    extra: number;
}>;
export declare const hatchetWithMiddlewareChaining: HatchetClient<GlobalInputType, {}, {
    dependency: string;
    first: number;
    second: number;
} & {
    anotherDep: boolean;
    first: number;
    second: number;
    dependency: string;
}, any>;
