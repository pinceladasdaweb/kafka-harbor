import type { RawMessage } from './adapter'
import type { Serializer } from './serializer'
import type { Message, MessageHeaders, RetryInfo } from './types'
import { SerializationError, isSerializationError } from './errors'

/**
 * Decodes a value through the serializer and owns the failure: whatever a
 * serializer throws on malformed bytes becomes a `SerializationError`, the
 * deterministic failure the pipelines route straight to the DLQ (or skip in
 * a redrive). A codec that throws its own class is otherwise mistaken for a
 * transient fault and walked through every retry level.
 */
export function deserializeValue (serializer: Serializer, bytes: Buffer, topic: string): unknown {
  try {
    return serializer.deserialize(bytes, topic)
  } catch (cause) {
    if (isSerializationError(cause)) throw cause
    throw new SerializationError(`message on topic "${topic}" could not be deserialized`, { cause })
  }
}

/**
 * The message a handler (or a redrive filter) sees, built from the wire
 * record. `deserializeTopic` is the topic the serializer is told about: the
 * original topic of the message, so a topic-aware serializer sees the same
 * name whether the bytes came from `orders`, `orders-retry-2` or the DLQ.
 */
export function toMessage (raw: RawMessage, headers: MessageHeaders, serializer: Serializer, deserializeTopic: string, retry: RetryInfo | undefined): Message {
  return {
    topic: raw.topic,
    partition: raw.partition,
    offset: raw.offset,
    key: raw.key === null ? null : raw.key.toString('utf8'),
    value: raw.value === null ? null : deserializeValue(serializer, raw.value, deserializeTopic),
    headers,
    timestamp: new Date(raw.timestamp),
    ...(retry !== undefined && { retry })
  }
}
