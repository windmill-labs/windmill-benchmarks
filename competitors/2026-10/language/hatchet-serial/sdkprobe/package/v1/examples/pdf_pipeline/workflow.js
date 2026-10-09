"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.pdfPipeline = void 0;
const fs_1 = require("fs");
const path_1 = require("path");
const os_1 = require("os");
const hatchet_client_1 = require("../hatchet-client");
// !!
const STOPWORDS = new Set([
    'a',
    'an',
    'and',
    'are',
    'as',
    'at',
    'be',
    'by',
    'for',
    'from',
    'in',
    'is',
    'it',
    'of',
    'on',
    'or',
    'that',
    'the',
    'to',
    'with',
]);
const MIN_WORD_LENGTH = 3;
const MAX_KEYWORDS = 6;
const FALLBACK_TEXT = 'Invoice from Acme Corp. Total amount due: 150 dollars. Payment terms: Net 30.';
/**
 * Extract text from a PDF buffer using pdf2json.
 * The file-path API is the most reliable path for this example.
 */
function extractPdfText(pdfBuffer) {
    return __awaiter(this, void 0, void 0, function* () {
        let PDFParser;
        try {
            // eslint-disable-next-line @typescript-eslint/no-require-imports
            PDFParser = require('pdf2json');
        }
        catch (_a) {
            // pdf2json not installed: return deterministic fallback text
            return { text: FALLBACK_TEXT, pageCount: 1 };
        }
        const tmpDir = (0, fs_1.mkdtempSync)((0, path_1.join)((0, os_1.tmpdir)(), 'hatchet-pdf-'));
        const tmpFile = (0, path_1.join)(tmpDir, 'input.pdf');
        (0, fs_1.writeFileSync)(tmpFile, pdfBuffer);
        try {
            const pdfData = yield new Promise((resolve, reject) => {
                const parser = new PDFParser();
                parser.on('pdfParser_dataReady', resolve);
                parser.on('pdfParser_dataError', (err) => reject(new Error(err.parserError)));
                parser.loadPDF(tmpFile);
            });
            const pageCount = pdfData.Pages.length;
            const text = pdfData.Pages.map((page) => page.Texts.map((t) => t.R.map((r) => decodeURIComponent(r.T)).join('')).join(' ')).join('\n');
            return { text, pageCount };
        }
        finally {
            try {
                (0, fs_1.rmSync)(tmpDir, { recursive: true, force: true });
            }
            catch (_b) {
                // best-effort cleanup
            }
        }
    });
}
// > Define the DAG
exports.pdfPipeline = hatchet_client_1.hatchet.workflow({
    name: 'pdf-pipeline',
});
// !!
// > Extract text task
const extractText = exports.pdfPipeline.task({
    name: 'extract_text',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        const decoded = Buffer.from(input.contentBase64, 'base64');
        const { text, pageCount } = yield extractPdfText(decoded);
        return { text, pageCount };
    }),
});
// !!
// > Classify task
const classifyDocument = exports.pdfPipeline.task({
    name: 'classify_document',
    parents: [extractText],
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const { text } = yield ctx.parentOutput(extractText);
        const lower = text.toLowerCase();
        let category;
        if (['invoice', 'amount due', 'payment', 'bill'].some((w) => lower.includes(w))) {
            category = 'invoice';
        }
        else if (['receipt', 'paid', 'transaction'].some((w) => lower.includes(w))) {
            category = 'receipt';
        }
        else if (['report', 'analysis', 'findings', 'conclusion'].some((w) => lower.includes(w))) {
            category = 'report';
        }
        else if (['dear', 'sincerely', 'regards'].some((w) => lower.includes(w))) {
            category = 'letter';
        }
        else {
            category = 'other';
        }
        return { category };
    }),
});
// !!
// > Summarize task
const summarizeText = exports.pdfPipeline.task({
    name: 'summarize_text',
    parents: [extractText],
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const { text } = yield ctx.parentOutput(extractText);
        const words = text.split(/\s+/).filter(Boolean);
        const maxWords = 50;
        let summary = words.slice(0, maxWords).join(' ');
        if (words.length > maxWords) {
            summary += '...';
        }
        return { summary, wordCount: words.length };
    }),
});
// !!
// > Extract keywords task
const extractKeywords = exports.pdfPipeline.task({
    name: 'extract_keywords',
    parents: [extractText],
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        var _a, _b;
        const { text } = yield ctx.parentOutput(extractText);
        const words = (_a = text.toLowerCase().match(/[a-z]+/g)) !== null && _a !== void 0 ? _a : [];
        const filtered = words.filter((w) => w.length >= MIN_WORD_LENGTH && !STOPWORDS.has(w));
        const counts = new Map();
        for (const w of filtered) {
            counts.set(w, ((_b = counts.get(w)) !== null && _b !== void 0 ? _b : 0) + 1);
        }
        const sorted = [...counts.entries()]
            .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
            .slice(0, MAX_KEYWORDS);
        return { keywords: sorted.map(([word]) => word) };
    }),
});
// !!
// > Format result task
exports.pdfPipeline.task({
    name: 'format_result',
    parents: [extractText, classifyDocument, summarizeText, extractKeywords],
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const extract = yield ctx.parentOutput(extractText);
        const classify = yield ctx.parentOutput(classifyDocument);
        const summary = yield ctx.parentOutput(summarizeText);
        const keywords = yield ctx.parentOutput(extractKeywords);
        return {
            filename: input.filename,
            category: classify.category,
            summary: summary.summary,
            keywords: keywords.keywords,
            wordCount: summary.wordCount,
            pageCount: extract.pageCount,
        };
    }),
});
// !!
