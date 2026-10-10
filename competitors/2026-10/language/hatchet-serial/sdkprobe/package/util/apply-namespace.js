"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.applyNamespace = applyNamespace;
function applyNamespace(name, namespace) {
    if (namespace && !name.startsWith(namespace)) {
        return `${namespace}${name}`;
    }
    return name;
}
