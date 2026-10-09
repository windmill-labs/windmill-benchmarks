type SecondsDuration = `${number}s`;
type MinutesDuration = `${number}m`;
type HoursDuration = `${number}h`;
type TwoUnitDurations = `${number}h${number}m` | `${number}h${number}s` | `${number}m${number}s`;
type ThreeUnitDurations = `${number}h${number}m${number}s`;
type DurationString = SecondsDuration | MinutesDuration | HoursDuration | TwoUnitDurations | ThreeUnitDurations;
export interface DurationObject {
    hours?: number;
    minutes?: number;
    seconds?: number;
}
/** A number is treated as milliseconds. */
export type Duration = DurationString | DurationObject | number;
/** Normalizes a Duration to Go-style string format (e.g. "1h30m5s"). */
export declare function durationToString(d: Duration): string;
export declare function durationToMs(d: Duration): number;
export {};
