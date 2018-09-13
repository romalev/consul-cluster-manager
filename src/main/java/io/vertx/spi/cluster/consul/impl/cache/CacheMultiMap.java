package io.vertx.spi.cluster.consul.impl.cache;


import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueList;
import io.vertx.ext.consul.Watch;
import io.vertx.spi.cluster.consul.impl.ChoosableSet;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static io.vertx.spi.cluster.consul.impl.ClusterSerializationUtils.decode;

/**
 * Implementation of local IN-MEMORY multimap cache which is essentially concurrent hash map under the hood.
 * Works in the same way as {@link CacheMap}.
 *
 * @author Roman Levytskyi
 */
public class CacheMultiMap<K, V> extends CacheListener {

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
        log.trace("Cache for: '{}' has been started.", name);
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
        log.trace("Getting all subs by: '{}' from internal cache.", k.toString());
        return cache.get(k);
    }

    /**
     * Puts an entry to internal cache.
     */
    public synchronized void put(K key, V value) {
        log.trace("Putting: '{}->'{}' to internal cache.", key, value);
        ChoosableSet<V> choosableSet = cache.get(key);
        if (choosableSet == null) choosableSet = new ChoosableSet<>(1);
        choosableSet.add(value);
        cache.put(key, choosableSet);
    }

    /**
     * Removes an entry from internal cache.
     */
    public synchronized void remove(K key, V value) {
        log.trace("Removing: '{}->'{}' from internal cache.", key, value);
        ChoosableSet<V> choosableSet = cache.get(key);
        if (choosableSet == null) return;
        choosableSet.remove(value);
        cache.put(key, choosableSet);
    }

    /**
     * Removes an entry from internal cache.
     */
    public void remove(KeyValue keyValue) {
        log.trace("Removing: '{}'->'{}' from internal cache.", keyValue.getKey(), keyValue.getValue());
        try {
            K key = (K) getEventBusAddress(keyValue.getKey());
            V value = decode(keyValue.getValue());
            remove(key, value);
        } catch (Exception e) {
            log.error("Exception occurred while updating the internal cache: '{}'. Exception details: '{}'.", name, e.getMessage());
        }
    }

    public void putAll(K key, ChoosableSet<V> values) {
        cache.put(key, values);
    }

    public boolean isEmpty() {
        return cache.isEmpty();
    }

    @Override
    protected void addOrUpdateEvent(KeyValue keyValue) {
        log.trace("Adding: '{}'->'{}' to internal cache.", keyValue.getKey(), keyValue.getValue());
        try {
            K key = (K) getEventBusAddress(keyValue.getKey());
            V value = decode(keyValue.getValue());
            put(key, value);
        } catch (Exception e) {
            log.error("Exception occurred while updating the internal cache: '{}'. Exception details: '{}'.", name, e.getMessage());
        }
    }

    @Override
    protected void removeEvent(KeyValue keyValue) {
        remove(keyValue);
    }

    @Override
    public String toString() {
        return cache.toString();
    }

    /**
     * Gets event bus address out of  multimap key - i.e. __vertx.subs/users.create.channel/{nodeId} -> users.create.channel.
     */
    private String getEventBusAddress(String key) {
        return key.substring(key.indexOf("/") + 1, key.lastIndexOf("/"));
    }
}
