/**
 * Confluent Schema Registry for a harbor, through the serdes of
 * @confluentinc/schemaregistry (an optional peer dependency): Avro, JSON
 * Schema and Protobuf, with the registry's own subject strategies, rules
 * and caching. The core never imports this module, and this module never
 * imports the registry client: it takes the serdes as built by the
 * application and fits them to the harbor's `Serializer`.
 */
import { AdapterError, ConfigError, SerializationError, describeError, isSerializationError, type Serializer } from '../index'

/** What a registry serializer looks like: `AvroSerializer`, `JsonSerializer` and `ProtobufSerializer` all have it. */
export interface RegistrySerializer<T = unknown> {
  serialize: (topic: string, value: T) => Promise<Buffer>
}

/** What a registry deserializer looks like: `AvroDeserializer`, `JsonDeserializer` and `ProtobufDeserializer` all have it. */
export interface RegistryDeserializer<T = unknown> {
  deserialize: (topic: string, bytes: Buffer) => Promise<T>
}

export interface SchemaRegistrySerializerOptions<T = unknown> {
  /** The producing side. Leave it out for a consumer-only serializer: a produce then fails with a ConfigError. */
  serializer?: RegistrySerializer<T>
  /** The consuming side. Leave it out for a producer-only serializer: a message then fails deterministically (a SerializationError with the ConfigError as cause), hence the DLQ. */
  deserializer?: RegistryDeserializer<T>
}

// What axios reports for a registry that cannot be reached, or whose
// certificate the process does not trust: the request never got an answer.
const CONNECTION_CODES = new Set([
  'ECONNREFUSED', 'ECONNRESET', 'ECONNABORTED', 'ETIMEDOUT', 'ENOTFOUND', 'EAI_AGAIN', 'EPIPE', 'EHOSTUNREACH', 'ENETUNREACH', 'ERR_NETWORK',
  'UNABLE_TO_VERIFY_LEAF_SIGNATURE', 'DEPTH_ZERO_SELF_SIGNED_CERT', 'SELF_SIGNED_CERT_IN_CHAIN', 'CERT_HAS_EXPIRED', 'ERR_TLS_CERT_ALTNAME_INVALID'
])
// The registry answering that it, or the caller's credentials, are in
// trouble: none of it is about the bytes.
const UNAVAILABLE_STATUSES = new Set([401, 403, 429])
// The client's OAuth layer reports a token it could not obtain as a plain
// Error with this message, before any request reaches the registry.
const TOKEN_FAILURE = /^Failed to get token from server/

/**
 * Whether a failure came from the registry, the network or the credentials
 * rather than from the value or the schema: an HTTP 5xx, 429, 401 or 403, a
 * connection or TLS error, or a bearer token that could not be obtained.
 * Anything else the registry client throws (a subject that does not exist,
 * a value the schema refuses, bytes without a known schema id) is
 * deterministic and would fail the same way on every attempt.
 */
export function isRegistryUnavailable (error: unknown): boolean {
  if (typeof error !== 'object' || error === null) return false
  const { status, code, message } = error as { status?: unknown, code?: unknown, message?: unknown }
  if (typeof status === 'number') return status >= 500 || UNAVAILABLE_STATUSES.has(status)
  if (typeof code === 'string' && CONNECTION_CODES.has(code)) return true
  return typeof message === 'string' && TOKEN_FAILURE.test(message)
}

/**
 * Fits the registry serdes to the harbor's `Serializer`, and classifies
 * their failures the way the pipelines need: a registry that is unavailable
 * is a transient fault (`AdapterError`, retryable, so the message walks the
 * retry ladder and is tried again), while a value the schema refuses, a
 * subject that does not exist, or bytes no schema describes are a
 * `SerializationError` (deterministic, straight to the DLQ). The topic the
 * harbor passes is the original topic of the message, so the registry sees
 * the same subject whether the bytes came from `orders` or `orders-retry-2`,
 * and a redrive reads the DLQ under the original topic as well.
 */
export function schemaRegistrySerializer<T = unknown> (options: SchemaRegistrySerializerOptions<T>): Serializer<T> {
  const { serializer, deserializer } = options
  if (serializer === undefined && deserializer === undefined) {
    throw new ConfigError('schemaRegistrySerializer needs a serializer, a deserializer, or both')
  }
  return {
    async serialize (value, topic) {
      if (serializer === undefined) throw new ConfigError(`schemaRegistrySerializer for "${topic}" has no serializer; it can only consume`)
      try {
        return await serializer.serialize(topic, value)
      } catch (cause) {
        throw classify(cause, `value for topic "${topic}" could not be serialized through the schema registry`)
      }
    },
    async deserialize (bytes, topic) {
      if (deserializer === undefined) throw new ConfigError(`schemaRegistrySerializer for "${topic}" has no deserializer; it can only produce`)
      try {
        return await deserializer.deserialize(topic, bytes)
      } catch (cause) {
        throw classify(cause, `message on topic "${topic}" could not be deserialized through the schema registry`)
      }
    }
  }
}

function classify (cause: unknown, what: string): Error {
  if (isSerializationError(cause)) return cause
  if (isRegistryUnavailable(cause)) return new AdapterError(`schema registry unavailable: ${describeError(cause)}`, { cause, retryable: true })
  return new SerializationError(`${what}: ${describeError(cause)}`, { cause })
}
