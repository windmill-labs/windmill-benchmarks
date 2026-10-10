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
// > Trigger the pipeline
const workflow_1 = require("./workflow");
const sample_pdf_1 = require("./sample-pdf");
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        const pdfBytes = (0, sample_pdf_1.makeSamplePdf)('Invoice from Acme Corp. Total amount due: 150 dollars. Payment terms: Net 30.');
        const input = {
            filename: 'sample-invoice.pdf',
            contentBase64: pdfBytes.toString('base64'),
        };
        const result = yield workflow_1.pdfPipeline.run(input);
        console.log('Pipeline result:', result);
    });
}
// !!
if (require.main === module) {
    main()
        .catch(console.error)
        .finally(() => process.exit(0));
}
