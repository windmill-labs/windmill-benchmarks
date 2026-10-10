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
const workflow_1 = require("./workflow");
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        // > Running a task with middleware
        const result = yield workflow_1.taskWithMiddleware.run({
            message: 'hello', // string  (from TaskInput)
            first: 1, // number  (from GlobalInputType)
            second: 2, // number  (from GlobalInputType)
        });
        console.log('result', result.message); // string  (from TaskOutput)
        console.log('result', result.extra); // number  (from GlobalOutputType)
        console.log('result', result.additionalData); // number  (from Post Middleware)
        // !!
    });
}
if (require.main === module) {
    main();
}
