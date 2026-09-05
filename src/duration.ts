import { ConfigError } from './errors'
import type { Duration } from './types'

const UNITS: Record<string, number> = {
  ms: 1,
  s: 1_000,
  m: 60_000,
  h: 3_600_000,
  d: 86_400_000
}

const PATTERN = /^(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)$/

/**
 * Parses a duration into milliseconds. Numbers are taken as milliseconds
 * and must be finite and non-negative; strings need a unit (`'5s'`, `'1m'`).
 * Bare strings of digits are rejected on purpose: `'5'` is more likely a
 * unit forgotten than five milliseconds meant.
 *
 * `name` is the option the value came from, so the error names what the
 * caller actually passed instead of an internal default.
 */
export function parseDuration (value: Duration, name: string): number {
  if (typeof value === 'number') {
    if (!Number.isFinite(value) || value < 0) {
      throw new ConfigError(`${name} must be a finite, non-negative number of milliseconds; got ${String(value)}`)
    }
    return value
  }
  if (typeof value !== 'string') {
    throw new ConfigError(`${name} must be a number of milliseconds or a duration string such as '5s'; got ${typeof value}`)
  }
  const match = PATTERN.exec(value.trim())
  if (match === null) {
    throw new ConfigError(`${name} must be a duration string such as '250ms', '5s', '1m', '2h' or '1d'; got '${value}'`)
  }
  const amount = Number(match[1])
  const unit = UNITS[match[2] as string] as number
  return Math.round(amount * unit)
}
