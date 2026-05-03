/**
 * Subscription event sink interface.
 *
 * This module defines the interface for consumers of change log events,
 * typically the SubscriptionManager. This abstraction allows the change poller
 * to be tested without requiring the full subscription manager.
 */
/* eslint-disable no-unused-vars */
import type { ChangeEventRow } from "../store/registryPort.js";

/**
 * Interface for consuming change log events.
 * Implementations receive change events and dispatch them to relevant subscribers.
 */
export interface SubscriptionEventSink {
  /**
   * Dispatches a change event to subscribers.
   */
  dispatchEvent(...args: [ChangeEventRow]): void;
}
