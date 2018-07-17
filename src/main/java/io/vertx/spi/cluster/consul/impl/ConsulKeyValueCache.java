package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Vertx;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.ConsulClientOptions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A utility that attempts to keep all data from all created (by node) Consul MAPS locally cached. This class
 * will watch the every consul map (by name), respond to update/create/delete events, pull down the data, re-create the maps if there was a connection
 * issue with Consul Agent.
 * <p>
 *
 * <p>
 * Correlations : node(sessionId) -> maps.
 */
public class ConsulKeyValueCache<K, V> extends ConsulMap<K, V> implements Map<K, V> {

    // We need a way to keep all the KV data cached locally.
    // 1. watches for every map created in consul.
    // 2. scan with specified interval the map to locate data incosistency (consider separate thread pool for it.)

    private final Map<K, V> CACHE = new ConcurrentHashMap<>();

    public ConsulKeyValueCache(Vertx vertx, ConsulClient consulClient, ConsulClientOptions consulClientOptions, String name, String sessionId) {
        super(consulClient, name, sessionId);
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean containsKey(Object key) {
        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        return false;
    }

    @Override
    public V get(Object key) {
        return null;
    }

    @Nullable
    @Override
    public V put(K key, V value) {
        return null;
    }

    @Override
    public V remove(Object key) {
        return null;
    }

    @Override
    public void putAll(@NotNull Map<? extends K, ? extends V> m) {

    }

    @Override
    public void clear() {

    }

    @NotNull
    @Override
    public Set<K> keySet() {
        return null;
    }

    @NotNull
    @Override
    public Collection<V> values() {
        return null;
    }

    @NotNull
    @Override
    public Set<Entry<K, V>> entrySet() {
        return null;
    }

}
