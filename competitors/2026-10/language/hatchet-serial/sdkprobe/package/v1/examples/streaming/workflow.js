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
exports.streamingTask = void 0;
const sleep_1 = __importDefault(require("../../../util/sleep"));
const hatchet_client_1 = require("../hatchet-client");
// > Streaming
const annaKarenina = `
Happy families are all alike; every unhappy family is unhappy in its own way.

Everything was in confusion in the Oblonskys' house. The wife had discovered that the husband was carrying on an intrigue with a French girl, who had been a governess in their family, and she had announced to her husband that she could not go on living in the same house with him.
`;
function* createChunks(content, n) {
    for (let i = 0; i < content.length; i += n) {
        yield content.slice(i, i + n);
    }
}
exports.streamingTask = hatchet_client_1.hatchet.task({
    name: 'stream-example',
    fn: (_, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        yield (0, sleep_1.default)(2000);
        for (const chunk of createChunks(annaKarenina, 10)) {
            ctx.putStream(chunk);
            yield (0, sleep_1.default)(200);
        }
    }),
});
// !!
