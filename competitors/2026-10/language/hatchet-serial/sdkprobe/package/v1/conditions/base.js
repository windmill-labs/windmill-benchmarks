"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Condition = exports.Action = void 0;
var Action;
(function (Action) {
    Action[Action["CREATE"] = 0] = "CREATE";
    Action[Action["QUEUE"] = 1] = "QUEUE";
    Action[Action["CANCEL"] = 2] = "CANCEL";
    Action[Action["SKIP"] = 3] = "SKIP";
    Action[Action["UNRECOGNIZED"] = -1] = "UNRECOGNIZED";
})(Action || (exports.Action = Action = {}));
class Condition {
    constructor(base) {
        this.base = base;
    }
}
exports.Condition = Condition;
