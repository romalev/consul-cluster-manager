package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Handler;
import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueList;
import io.vertx.ext.consul.WatchResult;
import io.vertx.spi.cluster.consul.impl.ConsulKvListener.EntryEvent.EventType;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Consul KV store listener encapsulates mechanism of receiving {@link io.vertx.ext.consul.KeyValueList} and transforming it to internal {@link EntryEvent}.
 *
 * @author Roman Levytskyi
 */
public interface ConsulKvListener {

  /**
   * Receives an event emitted by consul watch.
   *
   * @param event - holds the event's data.
   */
  void entryUpdated(EntryEvent event);

  /**
   * Transforms incoming {@link io.vertx.ext.consul.KeyValueList} into internal {@link EntryEvent}
   * <p>
   * Note: given approach provides O(n) execution time since it must loop through prev and next lists of entries.
   */
  default Handler<WatchResult<KeyValueList>> kvWatchHandler() {
    return event -> {
      Iterator<KeyValue> nextKvIterator = getKeyValueListOrEmptyList(event.nextResult()).iterator();
      Iterator<KeyValue> prevKvIterator = getKeyValueListOrEmptyList(event.prevResult()).iterator();

      Optional<KeyValue> prev = prevKvIterator.hasNext() ? Optional.of(prevKvIterator.next()) : Optional.empty();
      Optional<KeyValue> next = nextKvIterator.hasNext() ? Optional.of(nextKvIterator.next()) : Optional.empty();

      while (prev.isPresent() || next.isPresent()) {
        // prev and next exist
        if (prev.isPresent() && next.isPresent()) {
          // keys are equal
          if (prev.get().getKey().equals(next.get().getKey())) {
            if (prev.get().getModifyIndex() == next.get().getModifyIndex()) {
              // no update since keys AND their modify indices are equal.
            } else {
              entryUpdated(new EntryEvent(EventType.WRITE, next.get()));
            }
            prev = prevKvIterator.hasNext() ? Optional.of(prevKvIterator.next()) : Optional.empty();
            next = nextKvIterator.hasNext() ? Optional.of(nextKvIterator.next()) : Optional.empty();

          } else if (prev.get().getKey().compareToIgnoreCase(next.get().getKey()) > 0) {
            entryUpdated(new EntryEvent(EventType.WRITE, next.get()));
            next = nextKvIterator.hasNext() ? Optional.of(nextKvIterator.next()) : Optional.empty();
          } else {
            // ie -> evaluation this condition prev.get().getKey().compareToIgnoreCase(next.get().getKey()) < 0.
            entryUpdated(new EntryEvent(EventType.REMOVE, prev.get()));
            prev = prevKvIterator.hasNext() ? Optional.of(prevKvIterator.next()) : Optional.empty();
          }
          continue;
        }
        if (prev.isPresent()) {
          entryUpdated(new EntryEvent(EventType.REMOVE, prev.get()));
          prev = prevKvIterator.hasNext() ? Optional.of(prevKvIterator.next()) : Optional.empty();
          continue;
        }
        entryUpdated(new EntryEvent(EventType.WRITE, next.get()));
        next = nextKvIterator.hasNext() ? Optional.of(nextKvIterator.next()) : Optional.empty();
      }
    };
  }

  /**
   * Simple not-null wrapper around getting key value list. As a result returns either an empty list or actual key value list.
   */
  default List<KeyValue> getKeyValueListOrEmptyList(KeyValueList keyValueList) {
    return keyValueList == null || keyValueList.getList() == null ? Collections.emptyList() : keyValueList.getList();
  }

  /**
   * Represents an event that gets built out of consul watch's {@link io.vertx.ext.consul.KeyValueList}.
   */
  final class EntryEvent {
    private final EventType eventType;
    private final KeyValue entry;

    EntryEvent(EventType eventType, KeyValue entry) {
      this.eventType = eventType;
      this.entry = entry;
    }

    public EventType getEventType() {
      return eventType;
    }

    public KeyValue getEntry() {
      return entry;
    }

    // represents an event type
    public enum EventType {
      WRITE,
      REMOVE
    }
  }

}
