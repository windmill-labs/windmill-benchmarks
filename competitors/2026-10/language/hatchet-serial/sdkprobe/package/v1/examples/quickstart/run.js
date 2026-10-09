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
const first_task_1 = require("./workflows/first-task");
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        const res = yield first_task_1.firstTask.run({
            Message: 'Hello World!',
        });
        console.log('Finished running task, and got the transformed message! The transformed message is:', res.TransformedMessage);
    });
}
if (require.main === module) {
    main()
        .catch(console.error)
        .finally(() => process.exit(0));
}
