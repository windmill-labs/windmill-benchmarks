interface BatchedItem<T> {
    batchIndex: number;
    payloads: T[];
    originalIndices: number[];
}
export declare function batch<T>(payloads: T[], numElm: number, maxBytes: number): Array<BatchedItem<T>>;
export {};
