"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const sdk_1 = __importDefault(require("../../../sdk"));
const hatchet = sdk_1.default.init();
hatchet.events.push('rate-limit:create', {
    test: '1',
});
hatchet.events.push('rate-limit:create', {
    test: '2',
});
hatchet.events.push('rate-limit:create', {
    test: '3',
});
