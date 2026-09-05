/**
 * Kafka's default partitioner for keyed records: murmur2 over the key bytes,
 * masked to a positive 31-bit integer, modulo the partition count. The same
 * arithmetic the Java client and librdkafka use, so a key computed here lands
 * where a Kafka producer would send it, and the in-memory broker under
 * `kafka-harbor/testing` partitions the way a real one does.
 */
import { ConfigError } from './errors'

const SEED = 0x9747b28c
const M = 0x5bd1e995

/** Signed 32-bit multiply, the way Java does it. */
const mul32 = (a: number, b: number): number => Math.imul(a, b)

/** murmur2 (32-bit) of a byte sequence, as a signed 32-bit integer. */
export function murmur2 (data: Buffer): number {
  const length = data.length
  let h = SEED ^ length
  const length4 = length >>> 2
  for (let i = 0; i < length4; i++) {
    const i4 = i * 4
    let k = (data[i4] as number) | ((data[i4 + 1] as number) << 8) | ((data[i4 + 2] as number) << 16) | ((data[i4 + 3] as number) << 24)
    k = mul32(k, M)
    k ^= k >>> 24
    k = mul32(k, M)
    h = mul32(h, M)
    h ^= k
  }
  const tail = length % 4
  const base = length & ~3
  if (tail >= 3) h ^= (data[base + 2] as number) << 16
  if (tail >= 2) h ^= (data[base + 1] as number) << 8
  if (tail >= 1) {
    h ^= data[base] as number
    h = mul32(h, M)
  }
  h ^= h >>> 13
  h = mul32(h, M)
  h ^= h >>> 15
  return h
}

/**
 * The partition a keyed record goes to under Kafka's default partitioner.
 * Unkeyed records are the producer's business (round-robin or sticky); this
 * helper has no answer for them and says so.
 */
export function partitionForKey (key: string | Buffer, partitions: number): number {
  if (!Number.isInteger(partitions) || partitions < 1) {
    throw new ConfigError(`partitions must be an integer >= 1; got ${String(partitions)}`)
  }
  const bytes = typeof key === 'string' ? Buffer.from(key, 'utf8') : key
  return (murmur2(bytes) & 0x7fffffff) % partitions
}
