"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.runThreaded = runThreaded;
const worker_threads_1 = require("worker_threads");
const path_1 = __importDefault(require("path"));
function runThreaded(scriptPath, options) {
    const resolvedPath = require.resolve(scriptPath);
    const isTs = /\.ts$/.test(resolvedPath);
    // NOTE: if the file is typescript, we are in the SDK dev environment and need to so some funky work.
    // otherwise, we pass the file directly to the worker.
    const ex = isTs
        ? `
    const wk = require('worker_threads');
    require('tsconfig-paths/register');
    require('ts-node').register({
      "include": ["src/**/*.ts"],
      "exclude": ["./dist"],
      "compilerOptions": {
        "types": ["node"],
        "target": "es2016",
        "esModuleInterop": true,
        "module": "commonjs",
        "rootDir": "${path_1.default.join(__dirname, '../../../')}",
      }
    });
    let file = '${resolvedPath}';
    require(file);
    `
        : resolvedPath;
    return new worker_threads_1.Worker(ex, Object.assign(Object.assign({}, options), { eval: isTs ? true : undefined }));
}
// execArgv:  ? ['--require', 'ts-node/register'] : undefined,
