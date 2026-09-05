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

const assertEncodable = (value: unknown, path: string, seen: WeakSet<object>): void => {
  switch (typeof value) {
    case 'number':
      if (!Number.isFinite(value)) {
        throw new SerializationError(`${path} is ${String(value)}; JSON turns it into null`)
      }
      break
    case 'object': {
      if (value === null) return
      if (value instanceof Date) {
        if (Number.isNaN(value.getTime())) throw new SerializationError(`${path} is an invalid Date`)
        return
      }
      const rejected = isRejectedShape(value)
      if (rejected !== null) {
        throw new SerializationError(`${path} is a ${rejected}; JSON would not preserve it`)
      }
      if (seen.has(value)) throw new SerializationError(`${path} closes a cycle`)
      seen.add(value)
      if (Array.isArray(value)) {
        value.forEach((item, index) => assertEncodable(item, `${path}[${index}]`, seen))
      } else {
        for (const [key, item] of Object.entries(value)) {
          // `undefined` inside an object is dropped by JSON.stringify rather
          // than encoded; a consumer would see a missing key and never know
          // whether the producer meant to send it.
          assertEncodable(item, `${path}.${key}`, seen)
        }
      }
      seen.delete(value)
      break
    }
    case 'undefined':
    case 'function':
    case 'symbol':
    case 'bigint':
      throw new SerializationError(`${path} is a ${typeof value}; JSON has no representation for it`)
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
        assertEncodable(value, 'value', new WeakSet())
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
