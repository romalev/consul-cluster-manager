package io.vertx.spi.cluster.consul.impl.cache;

import io.vertx.core.Handler;
import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueList;
import io.vertx.ext.consul.WatchResult;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Dedicated cache listener based on watch - notifies exactly about  what has changed in consul kv store.
 *
 * @author Roman Levytskyi
 */
public abstract class CacheListener {

    /**
     * @param keyValue - represents key and value that was added to / updated in consul agent kv store.
     */
    protected abstract void addOrUpdateEvent(KeyValue keyValue);

    /**
     * @param keyValue - represents key and value an entry that was deleted in consul agent kv store.
     */
    protected abstract void removeEvent(KeyValue keyValue);

    /**
     * Implementation of watch handler to determine (listen for) updates that are happening within consul KV store.
     * <p>
     * Note: given approach provides O(n) execution time since it is must to loop through prev and next lists of entries.
     */
    Handler<WatchResult<KeyValueList>> kvWatchHandler() {
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
                            addOrUpdateEvent(next.get());
                        }
                        prev = prevKvIterator.hasNext() ? Optional.of(prevKvIterator.next()) : Optional.empty();
                        next = nextKvIterator.hasNext() ? Optional.of(nextKvIterator.next()) : Optional.empty();

                    } else if (prev.get().getKey().compareToIgnoreCase(next.get().getKey()) > 0) {
                        addOrUpdateEvent(next.get());
                        next = nextKvIterator.hasNext() ? Optional.of(nextKvIterator.next()) : Optional.empty();
                    } else {
                        // ie -> evaluation this condition prev.get().getKey().compareToIgnoreCase(next.get().getKey()) < 0.
                        removeEvent(prev.get());
                        prev = prevKvIterator.hasNext() ? Optional.of(prevKvIterator.next()) : Optional.empty();
                    }
                    continue;
                }
                if (prev.isPresent()) {
                    removeEvent(prev.get());
                    prev = prevKvIterator.hasNext() ? Optional.of(prevKvIterator.next()) : Optional.empty();
                    continue;
                }
                addOrUpdateEvent(next.get());
                next = nextKvIterator.hasNext() ? Optional.of(nextKvIterator.next()) : Optional.empty();
            }
        };
    }

    /**
     * Simple not-null wrapper around getting key value list. As a result returns either an empty list or actual key value list.
     */
    private List<KeyValue> getKeyValueListOrEmptyList(KeyValueList keyValueList) {
        return keyValueList == null || keyValueList.getList() == null ? Collections.emptyList() : keyValueList.getList();
    }

}
