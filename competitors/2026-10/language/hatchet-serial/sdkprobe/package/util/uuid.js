"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.isValidUUID = isValidUUID;
const v4_1 = require("zod/v4");
function isValidUUID(uuid) {
    return v4_1.z.uuid().safeParse(uuid).success;
}
