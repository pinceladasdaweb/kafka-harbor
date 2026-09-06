/**
 * Minimal typed event emitter, in the shape of breakwater's: a listener that
 * throws never breaks the pipeline that emitted, and the listener set is
 * snapshotted per emit so subscribing during an emit does not affect the
 * event in flight.
 */
/**
 * Any object type whose keys are the event names. Deliberately not a record
 * keyed by string: an interface extending one gains a string index
 * signature, and a misspelled event name would then type-check.
 */
export type EventMap = object
export type Listener<T> = (payload: T) => void

export interface Observable<E extends EventMap> {
  on: <K extends keyof E>(event: K, listener: Listener<E[K]>) => this
  off: <K extends keyof E>(event: K, listener: Listener<E[K]>) => this
}

export interface TypedEmitter<E extends EventMap> extends Observable<E> {
  emit: <K extends keyof E>(event: K, payload: E[K]) => void
}

export function createEmitter<E extends EventMap> (onListenerError: (error: unknown) => void): TypedEmitter<E> {
  const listeners = new Map<keyof E, Set<Listener<E[keyof E]>>>()
  const emitter: TypedEmitter<E> = {
    on (event, listener) {
      let set = listeners.get(event)
      if (set === undefined) {
        set = new Set()
        listeners.set(event, set)
      }
      set.add(listener as Listener<E[keyof E]>)
      return this
    },
    off (event, listener) {
      listeners.get(event)?.delete(listener as Listener<E[keyof E]>)
      return this
    },
    emit (event, payload) {
      const set = listeners.get(event)
      if (set === undefined) return
      for (const listener of [...set]) {
        try {
          listener(payload)
        } catch (error) {
          onListenerError(error)
        }
      }
    }
  }
  return emitter
}
