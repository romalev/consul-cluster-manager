package io.vertx.spi.cluster.consul.impl.cache;

import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.ext.consul.KeyValueList;
import io.vertx.ext.consul.Watch;

import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Dedicated mechanism that encapsulates managing caches and its watches.
 * Keeping nodes caches up-to-date is done by using consul watches.
 * Now:
 * - when node joins the cluster -> appropriate watches have to be started to listen for updates and cache them appropriately.
 * - when node leaves the cluster -> appropriate watches have to stopped.
 *
 * @author Roman Levytskyi
 */
public final class CacheManager {

    private boolean active;
    private final Map<String, Map<?, ?>> caches = new ConcurrentHashMap<>();
    // dedicated queue to store all the consul watches that belongs to a node - when a node leaves the cluster - all its appropriate watches are stopped.
    private final Queue<Watch> watches = new ConcurrentLinkedQueue<>();
    private final Vertx vertx;
    private final ConsulClientOptions cClOptns;
    private CacheMultiMap<?, ?> cacheMultiMap;

    public CacheManager(Vertx vertx, ConsulClientOptions cClOptns) {
        this.vertx = vertx;
        this.cClOptns = cClOptns;
        active = true;
    }

    /**
     * Closes given cache manager.
     */
    public void close() {
        // stopping all started watches.
        watches.forEach(Watch::stop);
        // caches eviction.
        caches.values().forEach(Map::clear);
        if (cacheMultiMap != null) cacheMultiMap.clear();
        active = false;
    }

    /**
     * @param name - cache's name
     * @return fully initialized map cache.
     */
    public <K, V> Map<K, V> createAndGetCacheMap(String name) {
        checkIfActive();
        return createAndGetCacheMap(name, Optional.empty());
    }

    /**
     * @param name - cache's name
     * @param map  - map's entries to be put directly put into the cache.
     * @return fully initialized map cache.
     */
    public <K, V> Map<K, V> createAndGetCacheMap(String name, Optional<Map<K, V>> map) {
        checkIfActive();
        return (Map<K, V>) caches.computeIfAbsent(name, key -> new CacheMap<>(name, createAndGetMapWatch(name), map));
    }

    public <K, V> CacheMultiMap<K, V> createAndGetCacheMultiMap(String name) {
        cacheMultiMap = new CacheMultiMap<>(name, createAndGetMapWatch(name));
        return (CacheMultiMap<K, V>) cacheMultiMap;
    }

    /**
     * Creates consul (kv store specific) watch.
     */
    public Watch<KeyValueList> createAndGetMapWatch(String mapName) {
        Watch<KeyValueList> kvWatch = Watch.keyPrefix(mapName, vertx, cClOptns);
        watches.add(kvWatch);
        return kvWatch;
    }

    /**
     * Checks whether given cache manager is active.
     *
     * @throws VertxException if cache manager is not active.
     */
    private void checkIfActive() {
        if (!active) {
            throw new VertxException("Cache manager is not active.");
        }
    }

}
