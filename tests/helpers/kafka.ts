import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka'
import { Wait } from 'testcontainers'

/**
 * A Kafka broker for the integration suite, started by Testcontainers in
 * KRaft mode (no ZooKeeper). Confluent Platform 8 ships Kafka 4.
 */
export const KAFKA_IMAGE = 'confluentinc/cp-kafka:8.0.0'

export interface KafkaFixture {
  brokers: string[]
  stop: () => Promise<void>
}

/**
 * Starts the broker, or explains why it could not. Without Docker the suite
 * skips, unless KAFKA_REQUIRED=1 (the CI integration job sets it) turns a
 * missing broker into a failure: a job that exists to run these tests must
 * never pass green by skipping them.
 */
export async function startKafka (): Promise<KafkaFixture | undefined> {
  let container: StartedKafkaContainer
  try {
    // The module's default readiness is the startup script being in place,
    // not the broker listening; wait for the broker's own "started" line so
    // the first connect does not race it.
    container = await new KafkaContainer(KAFKA_IMAGE)
      .withKraft()
      .withWaitStrategy(Wait.forLogMessage(/Kafka Server started/))
      .withStartupTimeout(180_000)
      .start()
  } catch (error) {
    if (process.env.KAFKA_REQUIRED === '1') throw error
    console.warn(`[integration] Kafka container unavailable, skipping (set KAFKA_REQUIRED=1 to fail instead): ${(error as Error).message}`)
    return undefined
  }
  const brokers = [`${container.getHost()}:${container.getMappedPort(9093)}`]
  return {
    brokers,
    stop: async () => { await container.stop() }
  }
}
