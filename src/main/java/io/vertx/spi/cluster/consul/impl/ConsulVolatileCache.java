package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueList;
import io.vertx.ext.consul.Watch;
import io.vertx.ext.consul.WatchResult;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static io.vertx.spi.cluster.consul.impl.ClusterSerializationUtils.decode;


/**
 * Implementation of READ-ONLY local IN-MEMORY cache (which is essentially concurrent hash map) that is being updated through consul watch.
 * Now:
 * - cache read operations happens synchronously by simple reading from {@link java.util.concurrent.ConcurrentHashMap}.
 * - cache write operations happens through consul watch that watches consul kv store updates (that seems to be the only way to acknowledge successful write operation to consul agent kv store).
 * <p>
 * Note: given cache implementation MIGHT NOT BE mature enough to handle different sort of failures that might occur (hey, that's distributed systems world :))
 *
 * @author Roman Levytskyi
 */
public class ConsulVolatileCache<K, V> implements Map<K, V> {

    private static final Logger log = LoggerFactory.getLogger(ConsulVolatileCache.class);

    private final Watch<KeyValueList> watch;
    private final String name;
    // true -> entries are to be encoded first and then put into the cache, false - plain entries are placed (as strings).
    private final boolean encodingEnabled;

    private Map<K, V> cache = new ConcurrentHashMap<>();

    /**
     * @param name            - cache's name -> should always correspond to the same map's name to which cache is applied.
     * @param encodingEnabled - true ->  entries are to be encoded first and then put into the cache, false -> plain entries are placed (as strings).
     * @param watch           - consul watch.
     * @param preBuiltCache   - optional: cache can be initialized with already pre-built one.
     */
    public ConsulVolatileCache(String name, boolean encodingEnabled, Watch<KeyValueList> watch, Optional<Map<K, V>> preBuiltCache) {
        this.name = name;
        this.watch = watch;
        this.encodingEnabled = encodingEnabled;
        preBuiltCache.ifPresent(kvMap -> cache.putAll(kvMap));
        start();
    }

    /**
     * Start caching data.
     */
    public void start() {
        log.trace("Cache for: '{}' has been started.", name);
        watch.setHandler(getMapHandler()).start();
    }

    /**
     * Evicts the cache.
     */
    public void evict() {
        cache.clear();
    }

    @Override
    public int size() {
        return cache.size();
    }

    @Override
    public boolean isEmpty() {
        return cache.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return cache.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return cache.containsValue(value);
    }

    /**
     * Returns an actual cached data.
     */
    public V get(Object key) {
        return cache.get(key);
    }

    @Nullable
    @Override
    public V put(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V remove(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(@NotNull Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        evict();
    }

    @NotNull
    @Override
    public Set<K> keySet() {
        return cache.keySet();
    }

    @NotNull
    @Override
    public Collection<V> values() {
        return cache.values();
    }

    @NotNull
    @Override
    public Set<Entry<K, V>> entrySet() {
        return cache.entrySet();
    }

    /**
     * Implementation of watch handler to determine updates that are happening within consul KV store.
     */
    private Handler<WatchResult<KeyValueList>> getMapHandler() {
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
                            log.trace("Entries are the same.");
                        } else {
                            log.trace("Entry: '{}' has been updated.", next.get().getKey());
                            addOrUpdateEntry(next.get());
                        }
                        prev = prevKvIterator.hasNext() ? Optional.of(prevKvIterator.next()) : Optional.empty();
                        next = nextKvIterator.hasNext() ? Optional.of(nextKvIterator.next()) : Optional.empty();

                    } else if (prev.get().getKey().compareToIgnoreCase(next.get().getKey()) > 0) {
                        log.trace("Entry: '{}' has been added.", next.get().getKey());
                        addOrUpdateEntry(next.get());
                        next = nextKvIterator.hasNext() ? Optional.of(nextKvIterator.next()) : Optional.empty();
                    } else {
                        // ie -> evaluation this condition prev.get().getKey().compareToIgnoreCase(next.get().getKey()) < 0.
                        log.trace("Entry: '{}' has been removed.", prev.get().getKey());
                        removeEntry(prev.get().getKey());
                        prev = prevKvIterator.hasNext() ? Optional.of(prevKvIterator.next()) : Optional.empty();
                    }
                    continue;
                }

                if (prev.isPresent()) {
                    log.trace("Entry: '{}' has been removed.", prev.get().getKey());
                    removeEntry(prev.get().getKey());
                    prev = prevKvIterator.hasNext() ? Optional.of(prevKvIterator.next()) : Optional.empty();
                    continue;
                }

                log.trace("Entry: '{}' has been added.", next.get().getKey());
                addOrUpdateEntry(next.get());
                next = nextKvIterator.hasNext() ? Optional.of(nextKvIterator.next()) : Optional.empty();
            }
        };
    }

    private void addOrUpdateEntry(KeyValue keyValue) {
        if (encodingEnabled) {
            try {
                K key = (K) keyValue.getKey().replace(name + "/", "");
                V value = decode(keyValue.getValue());
                cache.put(key, value);
            } catch (Exception e) {
                log.error("Exception occurred while updating the consul cache: '{}'. Exception details: '{}'.", name, e.getMessage());
            }
        } else {
            cache.put((K) keyValue.getKey(), (V) keyValue.getValue());
        }
    }

    private void removeEntry(String key) {
        cache.remove(key.replace(name + "/", ""));
    }

    /**
     * Simple not-null wrapper around getting key value list. As a result returns either an empty list or actual key value list.
     */
    private List<KeyValue> getKeyValueListOrEmptyList(KeyValueList keyValueList) {
        return keyValueList == null || keyValueList.getList() == null ? Collections.emptyList() : keyValueList.getList();
    }
}
