package io.vertx.spi.cluster.consul.impl.cache;

import io.vertx.core.Handler;
import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueList;
import io.vertx.ext.consul.WatchResult;
import io.vertx.spi.cluster.consul.impl.cache.KvStoreListener.Event.EventType;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Dedicated cache listener based on watch - notifies exactly about  what has changed in consul kv store.
 *
 * @author Roman Levytskyi
 */
public interface KvStoreListener {

    void event(Event event);

    /**
     * Implementation of watch handler to determine (listen for) updates that are happening within consul KV store.
     * <p>
     * Note: given approach provides O(n) execution time since it is must to loop through prev and next lists of entries.
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
                            event(new Event(EventType.WRITE, next.get()));
                        }
                        prev = prevKvIterator.hasNext() ? Optional.of(prevKvIterator.next()) : Optional.empty();
                        next = nextKvIterator.hasNext() ? Optional.of(nextKvIterator.next()) : Optional.empty();

                    } else if (prev.get().getKey().compareToIgnoreCase(next.get().getKey()) > 0) {
                        event(new Event(EventType.WRITE, next.get()));
                        next = nextKvIterator.hasNext() ? Optional.of(nextKvIterator.next()) : Optional.empty();
                    } else {
                        // ie -> evaluation this condition prev.get().getKey().compareToIgnoreCase(next.get().getKey()) < 0.
                        event(new Event(EventType.REMOVE, prev.get()));
                        prev = prevKvIterator.hasNext() ? Optional.of(prevKvIterator.next()) : Optional.empty();
                    }
                    continue;
                }
                if (prev.isPresent()) {
                    event(new Event(EventType.REMOVE, prev.get()));
                    prev = prevKvIterator.hasNext() ? Optional.of(prevKvIterator.next()) : Optional.empty();
                    continue;
                }
                event(new Event(EventType.WRITE, next.get()));
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
     * Represents an event being emitted from consul watch.
     */
    class Event {
        private final EventType eventType;
        private final KeyValue entry;
        Event(EventType eventType, KeyValue entry) {
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
