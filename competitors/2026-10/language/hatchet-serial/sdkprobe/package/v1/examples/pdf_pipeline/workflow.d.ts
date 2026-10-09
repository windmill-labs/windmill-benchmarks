export type PdfInput = {
    filename: string;
    contentBase64: string;
};
export type ExtractOutput = {
    text: string;
    pageCount: number;
};
export type ClassifyOutput = {
    category: string;
};
export type SummaryOutput = {
    summary: string;
    wordCount: number;
};
export type KeywordsOutput = {
    keywords: string[];
};
export type PipelineResult = {
    filename: string;
    category: string;
    summary: string;
    keywords: string[];
    wordCount: number;
    pageCount: number;
};
export declare const pdfPipeline: import("../..").WorkflowDeclaration<PdfInput, {}, {}>;
