package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.KeyValueList;
import io.vertx.ext.consul.KeyValueOptions;
import io.vertx.ext.consul.Watch;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static io.vertx.spi.cluster.consul.impl.ClusterSerializationUtils.decode;

/**
 * Distributed sync map implementation based on consul key-value store.
 * <p>
 * Given implementation fully relies on internal cache (consul client doesn't have any cache built-in so we are sort of forced to come up with something custom)
 * which essentially is {@link java.util.concurrent.ConcurrentHashMap}.
 * Now:
 * - cache read operations happens synchronously by simple reading from {@link java.util.concurrent.ConcurrentHashMap}.
 * - cache write operations happens through consul watch that watches consul kv store updates (that seems to be the only way to acknowledge successful write operation to consul agent kv store).
 * <p>
 * Note: given cache implementation MIGHT NOT BE mature enough to handle different sort of failures that might occur (hey, that's distributed systems world :))
 * <p>
 * Sync map's entry IS (MUST BE) EPHEMERAL.
 *
 * @author Roman Levytskyi
 */
public final class ConsulSyncMap<K, V> extends ConsulMap<K, V> implements Map<K, V> {

    private final static Logger log = LoggerFactory.getLogger(ConsulSyncMap.class);

    private final Map<K, V> cache;
    private final Vertx vertx;
    private final Watch<KeyValueList> watch;
    private final KeyValueOptions kvOptions;

    public ConsulSyncMap(String name, Vertx vx, ConsulClient cC, Watch<KeyValueList> watch, String sessionId, Map<K, V> cache) {
        super(name, cC);
        this.vertx = vx;
        this.watch = watch;
        this.cache = cache;
        // sync map's node mode should be EPHEMERAL, as lifecycle of its entries as long as verticle's.
        this.kvOptions = new KeyValueOptions().setAcquireSession(sessionId);
        watchHaInfoMap();
        printCache();
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

    @Override
    public V get(Object key) {
        return cache.get(key);
    }

    @Nullable
    @Override
    public V put(K key, V value) {
        putValue(key, value, kvOptions);
        return value;
    }

    @Override
    public V remove(Object key) {
        removeValue((K) key);
        return cache.get(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        log.trace("Putting: '{}' into Consul KV store.", Json.encodePrettily(m));
        m.forEach(this::putValue);
    }

    @Override
    public void clear() {
        clearUp();
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
     * Watch listens to events that are coming from Consul KV store and updates the internal cache appropriately.
     */
    private void watchHaInfoMap() {
        watch
                .setHandler(promise -> {
                    if (promise.succeeded()) {
                        // below is full cache update operation - this operation has be synchronized to NOT allow anyone else (any other threads) to read the cache while it's being updated.
                        // FIXME
                        synchronized (this) {
                            cache.clear();
                            if (promise.nextResult() != null && promise.nextResult().getList() != null && !promise.nextResult().getList().isEmpty()) {
                                promise.nextResult().getList().forEach(keyValue -> {
                                    try {
                                        K key = (K) keyValue.getKey().replace(name + "/", "");
                                        V value = decode(keyValue.getValue());
                                        cache.put(key, value);
                                    } catch (Exception e) {
                                        log.error("Exception occurred while updating the local map: '{}'. Exception details: '{}'.", name, e.getMessage());
                                        // don't throw any exceptions here - just ignore kv pair that can't be decoded.
                                    }
                                });
                            }
                        }
                        log.trace("Update to local cache of : '{}'  just happened and now cache is: '{}'.", name, Json.encodePrettily(cache));
                    } else {
                        log.error("Failed to register a watch. Details: '{}'", promise.cause().getMessage());
                    }
                })
                .start();
    }

    // just a dummy helper method [it's gonna get removed] to print out every 5 sec the data that resides within the internal cache.
    // TODO : removed it when consul cluster manager is more or less stable.
    private void printCache() {
        vertx.setPeriodic(15000, handler -> log.trace("Internal HaInfo (Sync) Map: '{}'", Json.encodePrettily(cache)));
    }
}
