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
exports.getTemperature = exports.getTemperatureWorkflow = void 0;
exports.createTemperatureWorkflowToolClaude = createTemperatureWorkflowToolClaude;
exports.createTemperatureWorkflowToolOpenai = createTemperatureWorkflowToolOpenai;
exports.createTemperatureTaskToolClaude = createTemperatureTaskToolClaude;
exports.createTemperatureTaskToolOpenai = createTemperatureTaskToolOpenai;
const hatchet_client_1 = require("../hatchet-client");
const v4_1 = require("zod/v4");
// > Models
// Agent tools require a Zod v4 inputValidator so the SDK can generate the tool input schema.
const TemperatureCoordinates = v4_1.z.object({
    latitude: v4_1.z.number(),
    longitude: v4_1.z.number(),
});
const TemperatureInput = v4_1.z.object({
    locationName: v4_1.z.string(),
    coords: TemperatureCoordinates,
});
// !!
const temperatureRequest = (input) => __awaiter(void 0, void 0, void 0, function* () {
    const response = yield fetch(`https://api.open-meteo.com/v1/forecast?latitude=${input.coords.latitude}&longitude=${input.coords.longitude}&current=temperature_2m&temperature_unit=fahrenheit`);
    const data = yield response.json();
    return {
        text: `Temperature in ${input.locationName}: ${data.current.temperature_2m}°F`,
    };
});
// > Workflow definition
exports.getTemperatureWorkflow = hatchet_client_1.hatchet.workflow({
    name: 'getTemperatureWorkflow',
    inputValidator: TemperatureInput,
    description: 'Get the current temperature at a location',
});
exports.getTemperatureWorkflow.task({
    name: 'getTemperature',
    fn: temperatureRequest,
});
// !!
// > Standalone task
exports.getTemperature = hatchet_client_1.hatchet.task({
    name: 'getTemperature',
    retries: 3,
    fn: temperatureRequest,
    inputValidator: TemperatureInput,
    description: 'Get the current temperature at a location',
});
// !!
// > Create MCP tools
function createTemperatureWorkflowToolClaude() {
    return exports.getTemperatureWorkflow.mcpTool('claude');
}
function createTemperatureWorkflowToolOpenai() {
    return exports.getTemperatureWorkflow.mcpTool('openai');
}
function createTemperatureTaskToolClaude() {
    return exports.getTemperature.mcpTool('claude');
}
function createTemperatureTaskToolOpenai() {
    return exports.getTemperature.mcpTool('openai');
}
// !!
