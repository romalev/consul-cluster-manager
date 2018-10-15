package io.vertx.spi.cluster.consul.impl;


import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.consul.KeyValueList;
import io.vertx.ext.consul.Watch;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static io.vertx.spi.cluster.consul.impl.ConversationUtils.asConsulEntry;

/**
 * Implementation of local IN-MEMORY multimap cache which is essentially concurrent hash map under the hood.
 * Works in the same way as {@link CacheMap}.
 *
 * @author Roman Levytskyi
 */
public final class CacheMultiMap<K, V> implements ConsulKvListener {

    private static final Logger log = LoggerFactory.getLogger(CacheMultiMap.class);
    private final String name;
    private final String nodeId;
    private final Watch<KeyValueList> watch;
    private ConcurrentMap<K, ChoosableSet<V>> cache = new ConcurrentHashMap<>();

    CacheMultiMap(String name, String nodeId, Watch<KeyValueList> watch) {
        this.name = name;
        this.nodeId = nodeId;
        this.watch = watch;
        start();
    }

    /**
     * Start caching data.
     */
    private void start() {
        log.trace("[" + nodeId + "]" + " Cache for: " + name + " has been started.");
        watch.setHandler(kvWatchHandler()).start();
    }

    /**
     * Evicts the cache.
     */
    void clear() {
        cache.clear();
    }

    boolean containsKey(K k) {
        return cache.containsKey(k);
    }

    /**
     * Gets an entry  from internal cache.
     */
    ChoosableSet<V> get(K k) {
        return cache.get(k);
    }

    /**
     * Puts an entry to internal cache.
     */
    synchronized void put(K key, V value) {
        ChoosableSet<V> choosableSet = cache.get(key);
        if (choosableSet == null) choosableSet = new ChoosableSet<>(1);
        choosableSet.add(value);
        cache.put(key, choosableSet);
        log.trace("[" + nodeId + "]" + " Cache: " + name + " after put of " + key + " -> " + value + ": " + this.toString());
    }

    /**
     * Removes an entry from internal cache.
     */
    synchronized void remove(K key, V value) {
        ChoosableSet<V> choosableSet = cache.get(key);
        if (choosableSet == null) return;
        choosableSet.remove(value);
        if (choosableSet.isEmpty()) cache.remove(key);
        else cache.put(key, choosableSet);
        log.trace("[" + nodeId + "]" + " Cache: " + name + " after remove of " + key + " -> " + value + ": " + this.toString());
    }

    synchronized void putAllForKey(K key, ChoosableSet<V> values) {
        cache.put(key, values);
    }

    ConcurrentMap<K, ChoosableSet<V>> get() {
        return cache;
    }

    boolean isEmpty() {
        return cache.isEmpty();
    }

    @Override
    public void entryUpdated(EntryEvent event) {
        log.trace("[" + nodeId + "]" + " Entry: " + event.getEntry().getKey() + " is " + event.getEventType());
        ConsulEntry<K, Set<V>> entry;
        try {
            entry = asConsulEntry(event.getEntry().getValue());
        } catch (Exception e) {
            log.error("Failed to decode: " + event.getEntry().getKey() + " -> " + event.getEntry().getValue(), e);
            return;
        }
        switch (event.getEventType()) {
            case WRITE:
                entry.getValue().forEach(v -> put(entry.getKey(), v));
                break;
            case REMOVE:
                entry.getValue().forEach(v -> remove(entry.getKey(), v));
                break;
            default:
                break;
        }
    }

    @Override
    public String toString() {
        return Json.encodePrettily(cache);
    }
}
