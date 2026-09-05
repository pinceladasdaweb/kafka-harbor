// The identifiers the documentation leaves undefined on purpose: a snippet
// about the consumer should not have to define the order type it handles.
// Declared once here, as globals, so every snippet module sees them; a
// snippet that declares its own `harbor` or `consumer` simply shadows these.
// Only scripts/check-doc-snippets.mjs loads this file.
import type { AsyncLocalStorage } from 'node:async_hooks'

import type { ClientAdapter, Consumer, Handler, Harbor, Serializer } from 'kafka-harbor'

declare global {
  interface Order { id: string, customerId: string, total: number }
  interface OrderEvent { type: string, orderId: string }
  class Payment { orderId: string; amount: number }
  interface MyType { id: string }
  class ValidationError extends Error {}

  const harbor: Harbor
  const consumer: Consumer
  const brokers: string[]
  const run: string
  const order: Order
  const orderA: Order
  const orderB: Order
  const schema: unknown

  function fulfill (order: Order): Promise<void>
  function avroSerializer (schema: unknown): Serializer<OrderEvent>
  function protobufSerializer (type: typeof Payment): Serializer<Payment>
  function encode (value: MyType): Buffer
  function decode (bytes: Buffer): MyType
  function alert (message: string): void
  function myAdapter (): ClientAdapter
  function randomUUID (): string

  const onOrder: Handler<Order>
  const onPayment: Handler<Payment>
  const handler: Handler<string>
  const asyncLocalStorage: AsyncLocalStorage<{ requestId: string }>
}

export {}
