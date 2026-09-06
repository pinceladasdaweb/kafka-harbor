import { ConfigError } from './errors'

/** Rejects anything but a non-empty string; `name` is the option the value came from. */
export function requireNonEmptyString (value: unknown, name: string): string {
  if (typeof value !== 'string' || value === '') {
    throw new ConfigError(`${name} must be a non-empty string; got ${typeof value === 'string' ? '""' : typeof value}`)
  }
  return value
}

/** Rejects anything but an integer of at least one. */
export function requirePositiveInteger (value: unknown, name: string): number {
  // Number.isInteger is false for anything that is not a number.
  if (!Number.isInteger(value) || (value as number) < 1) {
    throw new ConfigError(`${name} must be an integer >= 1; got ${String(value)}`)
  }
  return value as number
}

/** The first rejection among settled results, if any. */
export function firstRejection (results: readonly PromiseSettledResult<unknown>[]): PromiseRejectedResult | undefined {
  return results.find((result): result is PromiseRejectedResult => result.status === 'rejected')
}
