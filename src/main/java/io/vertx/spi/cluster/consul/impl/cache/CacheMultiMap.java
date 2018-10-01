package io.vertx.spi.cluster.consul.impl.cache;


import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.consul.KeyValueList;
import io.vertx.ext.consul.Watch;
import io.vertx.spi.cluster.consul.impl.ChoosableSet;
import io.vertx.spi.cluster.consul.impl.ConversationUtils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static io.vertx.spi.cluster.consul.impl.ConversationUtils.decode;

/**
 * Implementation of local IN-MEMORY multimap cache which is essentially concurrent hash map under the hood.
 * Works in the same way as {@link CacheMap}.
 *
 * @author Roman Levytskyi
 */
public final class CacheMultiMap<K, V> implements KvStoreListener {

    private static final Logger log = LoggerFactory.getLogger(CacheMultiMap.class);
    private final String name;
    private final Watch<KeyValueList> watch;
    private ConcurrentMap<K, ChoosableSet<V>> cache = new ConcurrentHashMap<>();

    CacheMultiMap(String name, Watch<KeyValueList> watch) {
        this.name = name;
        this.watch = watch;
        start();
    }

    /**
     * Start caching data.
     */
    private void start() {
        log.trace("Cache for: " + name + " has been started.");
        watch.setHandler(kvWatchHandler()).start();
    }

    /**
     * Evicts the cache.
     */
    public void clear() {
        cache.clear();
    }

    /**
     * Gets an entry  from internal cache.
     */
    public ChoosableSet<V> get(K k) {
        return cache.get(k);
    }

    /**
     * Puts an entry to internal cache.
     */
    public synchronized void put(K key, V value) {
        ChoosableSet<V> choosableSet = cache.get(key);
        if (choosableSet == null) choosableSet = new ChoosableSet<>(1);
        choosableSet.add(value);
        cache.put(key, choosableSet);
        log.trace("Cache: " + name + " after put of " + key + " -> " + value + ": " + this.toString());
    }

    /**
     * Removes an entry from internal cache.
     */
    public synchronized void remove(K key, V value) {
        ChoosableSet<V> choosableSet = cache.get(key);
        if (choosableSet == null) return;
        choosableSet.remove(value);
        if (choosableSet.isEmpty()) cache.remove(key);
        else cache.put(key, choosableSet);
        log.trace("Cache: " + name + " after remove of " + key + " -> " + value + ": " + this.toString());
    }

    public void putAll(K key, ChoosableSet<V> values) {
        cache.put(key, values);
    }

    public boolean isEmpty() {
        return cache.isEmpty();
    }

    @Override
    public void entryUpdated(EntryEvent event) {
        ConversationUtils.GenericEntry<K, V> entry;
        try {
            entry = decode(event.getEntry().getValue());
        } catch (Exception e) {
            log.error("Failed to decode: " + event.getEntry().getKey() + " -> " + event.getEntry().getValue(), e);
            return;
        }
        switch (event.getEventType()) {
            case WRITE:
                put(entry.getKey(), entry.getValue());
                break;
            case REMOVE:
                remove(entry.getKey(), entry.getValue());
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
