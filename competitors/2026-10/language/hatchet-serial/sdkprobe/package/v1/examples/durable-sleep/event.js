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
const hatchet_client_1 = require("../hatchet-client");
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        const event = yield hatchet_client_1.hatchet.events.push('user:event', {
            Data: { Hello: 'World' },
        });
    });
}
if (require.main === module) {
    main()
        .then(() => process.exit(0))
        .catch((error) => {
        console.error('Error:', error);
        process.exit(1);
    });
}
