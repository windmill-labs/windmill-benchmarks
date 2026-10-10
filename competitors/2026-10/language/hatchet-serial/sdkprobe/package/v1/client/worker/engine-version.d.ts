export declare const MinEngineVersion: {
    readonly SLOT_CONFIG: "v0.78.23";
    readonly DURABLE_EVICTION: "v0.80.0";
};
export declare function supportsEviction(engineVersion: string | undefined): boolean;
