/**
 * One string per topic partition, for maps keyed by both. The separator is a
 * character no topic name may contain, so `a` partition 10 and `a 1` partition
 * 0 never collide the way a space-joined key would let them.
 */
export const partitionKey = (topic: string, partition: number): string => `${topic}\u0000${partition}`

/** The two halves of a `partitionKey`. */
export const splitPartitionKey = (key: string): { topic: string, partition: number } => {
  const at = key.lastIndexOf('\u0000')
  return { topic: key.slice(0, at), partition: Number(key.slice(at + 1)) }
}
