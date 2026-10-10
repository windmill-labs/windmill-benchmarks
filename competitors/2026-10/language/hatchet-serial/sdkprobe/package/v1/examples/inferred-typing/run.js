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
        const declaredTypeRun = workflow_1.declaredType.run({
            Message: 'hello',
        });
        const inferredTypeRun = workflow_1.inferredType.run({
            Message: 'hello',
        });
        const crazyWorkflowRun = workflow_1.crazyWorkflow.run({
            Message: 'hello',
        });
        const inferredTypeDurableRun = workflow_1.inferredTypeDurable.run({
            Message: 'Durable Task',
        });
        const [declaredTypeResult, inferredTypeResult, inferredTypeDurableResult, crazyWorkflowResult] = yield Promise.all([declaredTypeRun, inferredTypeRun, inferredTypeDurableRun, crazyWorkflowRun]);
        console.log('declaredTypeResult', declaredTypeResult);
        console.log('inferredTypeResult', inferredTypeResult);
        console.log('inferredTypeDurableResult', inferredTypeDurableResult);
        console.log('crazyWorkflowResult', crazyWorkflowResult);
        console.log('declaredTypeResult.TransformedMessage', declaredTypeResult.TransformedMessage);
        console.log('inferredTypeResult.TransformedMessage', inferredTypeResult.TransformedMessage);
        console.log('inferredTypeDurableResult.TransformedMessage', inferredTypeDurableResult.TransformedMessage);
        console.log('crazyWorkflowResult.TransformedMessage', crazyWorkflowResult.TransformedMessage);
    });
}
if (require.main === module) {
    main();
}
