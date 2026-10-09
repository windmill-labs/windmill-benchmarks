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
exports.withErrorHandling = exports.parentSingleChild = exports.parent = exports.child = void 0;
// > Declaring a Child
const hatchet_client_1 = require("../hatchet-client");
exports.child = hatchet_client_1.hatchet.task({
    name: 'child',
    fn: (input) => {
        return {
            Value: input.N,
        };
    },
});
exports.parent = hatchet_client_1.hatchet.task({
    name: 'parent',
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const n = input.N;
        const promises = [];
        for (let i = 0; i < n; i++) {
            promises.push(exports.child.run({ N: i }));
        }
        const childRes = yield Promise.all(promises);
        const sum = childRes.reduce((acc, curr) => acc + curr.Value, 0);
        return {
            Result: sum,
        };
    }),
});
// !!
// > Parent with Single Child
exports.parentSingleChild = hatchet_client_1.hatchet.task({
    name: 'parent-single-child',
    fn: () => __awaiter(void 0, void 0, void 0, function* () {
        const childRes = yield exports.child.run({ N: 1 });
        return {
            Result: childRes.Value,
        };
    }),
});
// !!
// > Parent with Error Handling
exports.withErrorHandling = hatchet_client_1.hatchet.task({
    name: 'parent-error-handling',
    fn: () => __awaiter(void 0, void 0, void 0, function* () {
        try {
            const childRes = yield exports.child.run({ N: 1 });
            return {
                Result: childRes.Value,
            };
        }
        catch (error) {
            // decide how to proceed here
            return {
                Result: -1,
            };
        }
    }),
});
// !!
