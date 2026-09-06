import { SerializationError, isSerializationError } from './errors'

/**
 * Turns a value into wire bytes and back. Symmetric by contract: the
 * consumer deserializes with the same serializer the producer used.
 *
 * Implementations must fail loudly: a value that cannot be encoded
 * faithfully raises `SerializationError`, it is never silently flattened.
 */
export interface Serializer<T = unknown> {
  serialize: (value: T, topic: string) => Buffer
  deserialize: (bytes: Buffer, topic: string) => T
}

/**
 * Shapes JSON.stringify would turn into `{}` or `null` without a word, and
 * shapes it would mangle through `toJSON` before a replacer could see them.
 * Rejecting them is the difference between "the handler gets what the
 * producer meant" and "the handler gets an empty object".
 */
const isRejectedShape = (value: object): string | null => {
  if (value instanceof Map) return 'Map'
  if (value instanceof Set) return 'Set'
  if (value instanceof WeakMap) return 'WeakMap'
  if (value instanceof WeakSet) return 'WeakSet'
  if (value instanceof RegExp) return 'RegExp'
  if (value instanceof Error) return 'Error'
  if (value instanceof Promise) return 'Promise'
  if (ArrayBuffer.isView(value)) return 'typed array or Buffer'
  if (value instanceof ArrayBuffer) return 'ArrayBuffer'
  return null
}

/**
 * Where in the value a rejected node sits, collected while the failure
 * unwinds: the path is only ever needed in the error message, so the happy
 * path allocates nothing for it.
 */
class Unencodable {
  readonly segments: string[] = []
  constructor (readonly reason: string) {}

  message (): string {
    return `value${this.segments.reverse().join('')} ${this.reason}`
  }
}

const walk = (value: unknown, seen: WeakSet<object>): void => {
  switch (typeof value) {
    case 'number':
      if (!Number.isFinite(value)) throw new Unencodable(`is ${String(value)}; JSON turns it into null`)
      break
    case 'object': {
      if (value === null) return
      if (value instanceof Date) {
        if (Number.isNaN(value.getTime())) throw new Unencodable('is an invalid Date')
        return
      }
      const rejected = isRejectedShape(value)
      if (rejected !== null) throw new Unencodable(`is a ${rejected}; JSON would not preserve it`)
      if (seen.has(value)) throw new Unencodable('closes a cycle')
      seen.add(value)
      if (Array.isArray(value)) {
        for (let index = 0; index < value.length; index++) descend(value[index], `[${index}]`, seen)
      } else {
        // `undefined` inside an object is dropped by JSON.stringify rather
        // than encoded; a consumer would see a missing key and never know
        // whether the producer meant to send it.
        for (const key in value) descend((value as Record<string, unknown>)[key], `.${key}`, seen)
      }
      seen.delete(value)
      break
    }
    case 'undefined':
    case 'function':
    case 'symbol':
    case 'bigint':
      throw new Unencodable(`is a ${typeof value}; JSON has no representation for it`)
  }
}

const descend = (item: unknown, segment: string, seen: WeakSet<object>): void => {
  try {
    walk(item, seen)
  } catch (failure) {
    if (failure instanceof Unencodable) failure.segments.push(segment)
    throw failure
  }
}

const assertEncodable = (value: unknown): void => {
  try {
    walk(value, new WeakSet())
  } catch (failure) {
    if (failure instanceof Unencodable) throw new SerializationError(failure.message())
    throw failure
  }
}

/**
 * The default serializer: JSON, strict. `Date` is the one conversion accepted
 * (encoded as ISO-8601, decoded as a string, since JSON carries no type).
 * Everything JSON would silently flatten is rejected with
 * `SerializationError` before any byte is produced.
 */
export function jsonSerializer<T = unknown> (): Serializer<T> {
  return {
    serialize (value, topic) {
      try {
        assertEncodable(value)
        return Buffer.from(JSON.stringify(value), 'utf8')
      } catch (cause) {
        if (isSerializationError(cause)) throw cause
        // A getter that throws, or a value whose shape changed under us.
        throw new SerializationError(`value for topic "${topic}" could not be encoded as JSON`, { cause })
      }
    },
    deserialize (bytes, topic) {
      try {
        return JSON.parse(bytes.toString('utf8')) as T
      } catch (cause) {
        throw new SerializationError(`message on topic "${topic}" is not valid JSON`, { cause })
      }
    }
  }
}

/** Passes bytes through untouched, for opaque payloads and custom codecs. */
export function rawSerializer (): Serializer<Buffer> {
  return {
    serialize (value, topic) {
      if (!Buffer.isBuffer(value)) {
        throw new SerializationError(`rawSerializer expects a Buffer for topic "${topic}"; got ${typeof value}`)
      }
      return value
    },
    deserialize: (bytes) => bytes
  }
}

/** UTF-8 strings, for text payloads that are not JSON. */
export function stringSerializer (): Serializer<string> {
  return {
    serialize (value, topic) {
      if (typeof value !== 'string') {
        throw new SerializationError(`stringSerializer expects a string for topic "${topic}"; got ${typeof value}`)
      }
      return Buffer.from(value, 'utf8')
    },
    deserialize: (bytes) => bytes.toString('utf8')
  }
}
