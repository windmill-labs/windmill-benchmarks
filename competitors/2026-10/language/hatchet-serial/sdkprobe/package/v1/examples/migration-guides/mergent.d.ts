export declare function processImageTask(req: {
    body: {
        imageUrl: string;
        filters: string[];
    };
}): Promise<{
    success: boolean;
    processedUrl: string;
}>;
type ImageProcessInput = {
    imageUrl: string;
    filters: string[];
};
type ImageProcessOutput = {
    processedUrl: string;
    metadata: {
        size: number;
        format: string;
        appliedFilters: string[];
    };
};
export declare const imageProcessor: import("../..").TaskWorkflowDeclaration<ImageProcessInput, ImageProcessOutput, {}, {}, {}, {}>;
export {};
