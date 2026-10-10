"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.durationToString = durationToString;
exports.durationToMs = durationToMs;
const DURATION_RE = /^(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$/;
/** Normalizes a Duration to Go-style string format (e.g. "1h30m5s"). */
function durationToString(d) {
    if (typeof d === 'string')
        return d;
    if (typeof d === 'number') {
        const totalSeconds = Math.floor(d / 1000);
        const h = Math.floor(totalSeconds / 3600);
        const m = Math.floor((totalSeconds % 3600) / 60);
        const s = totalSeconds % 60;
        let out = '';
        if (h)
            out += `${h}h`;
        if (m)
            out += `${m}m`;
        if (s || !out)
            out += `${s}s`;
        return out;
    }
    let s = '';
    if (d.hours)
        s += `${d.hours}h`;
    if (d.minutes)
        s += `${d.minutes}m`;
    if (d.seconds)
        s += `${d.seconds}s`;
    return s || '0s';
}
function durationToMs(d) {
    var _a, _b, _c;
    if (typeof d === 'number')
        return d;
    if (typeof d === 'object') {
        return (((_a = d.hours) !== null && _a !== void 0 ? _a : 0) * 3600 + ((_b = d.minutes) !== null && _b !== void 0 ? _b : 0) * 60 + ((_c = d.seconds) !== null && _c !== void 0 ? _c : 0)) * 1000;
    }
    const match = d.match(DURATION_RE);
    if (!match) {
        throw new Error(`Invalid duration string: "${d}". Expected format like "1h30m5s", "10m", "30s".`);
    }
    const [, h, m, s] = match;
    return ((parseInt(h !== null && h !== void 0 ? h : '0', 10) * 3600 + parseInt(m !== null && m !== void 0 ? m : '0', 10) * 60 + parseInt(s !== null && s !== void 0 ? s : '0', 10)) * 1000);
}
