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
import java.util.Optional;
import java.util.Set;

/**
 * Distributed sync map implementation based on consul key-value store.
 * <p>
 * Given implementation fully relies on internal cache (consul client doesn't have any cache built-in so we are sort of forced to come up with something custom)
 * which essentially is {@link java.util.concurrent.ConcurrentHashMap}.
 * <p>
 * Sync map's entry IS (MUST BE) EPHEMERAL.
 *
 * @author Roman Levytskyi
 */
public final class ConsulSyncMap<K, V> extends ConsulMap<K, V> implements Map<K, V> {

    private final static Logger log = LoggerFactory.getLogger(ConsulSyncMap.class);

    private final Vertx vertx;
    private final KeyValueOptions kvOptions;
    private final Map<K, V> cache;

    public ConsulSyncMap(String name, Vertx vx, ConsulClient cC, Watch<KeyValueList> watch, String sessionId, Map<K, V> haInfo) {
        super(name, cC);
        this.vertx = vx;
        this.cache = new ConsulVolatileCache<>(name, true, watch, Optional.of(haInfo));
        // sync map's node mode should be EPHEMERAL, as lifecycle of its entries as long as verticle's.
        this.kvOptions = new KeyValueOptions().setAcquireSession(sessionId);
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
        putValue(key, value, kvOptions).setHandler(event -> {
            // in case of network drop - retry.
            if (event.failed()) {
                log.warn("KV: '{}->'{}' has NOT been put to Consul. Retrying...'", key, value);
                put(key, value);
            }
        });
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


    // just a dummy helper method [it's gonna get removed] to print out every 5 sec the data that resides within the internal cache.
    // TODO : removed it when consul cluster manager is more or less stable.
    private void printCache() {
        vertx.setPeriodic(15000, handler -> log.trace("Internal HaInfo (Sync) Map: '{}'", Json.encodePrettily(cache)));
    }
}
