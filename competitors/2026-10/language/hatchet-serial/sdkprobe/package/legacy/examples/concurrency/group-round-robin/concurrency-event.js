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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const sdk_1 = __importDefault(require("../../../../sdk"));
const hatchet = sdk_1.default.init();
const sleep = (ms) => new Promise((resolve) => {
    setTimeout(resolve, ms);
});
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        for (let i = 0; i < 20; i++) {
            let group = 0;
            if (i > 10) {
                group = 1;
            }
            hatchet.events.push('concurrency:create', {
                data: `event ${i}`,
                group,
            });
        }
    });
}
main();
