package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.ext.consul.KeyValueList;
import io.vertx.ext.consul.ServiceList;
import io.vertx.ext.consul.Watch;

import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Cache manager implementation - special mechanism to manage (create and evict) caches being used by consul cluster manager (consul distributed maps).
 *
 * @author Roman Levytskyi
 */
public class CacheManager {

    private static CacheManager instance;
    private static boolean active;
    private final Map<String, Map<?, ?>> caches = new ConcurrentHashMap<>();
    // dedicated queue to store all the consul watches that belongs to a node - when a node leaves the cluster - all its appropriate watches must be stopped.
    private final Queue<Watch> watches = new ConcurrentLinkedQueue<>();
    private final Vertx vertx;
    private final ConsulClientOptions cClOptns;

    /**
     * Initializes given cache manager.
     *
     * @param vertx    - vertx object.
     * @param cClOptns - consul client options.
     */
    public static void init(Vertx vertx, ConsulClientOptions cClOptns) {
        instance = new CacheManager(vertx, cClOptns);
    }

    /**
     * Closes given cache manager.
     */
    public static void close() {
        // stopping all started watches.
        instance.watches.forEach(Watch::stop);
        // caches eviction.
        instance.caches.values().forEach(Map::clear);
        active = false;
    }

    private CacheManager(Vertx vertx, ConsulClientOptions cClOptns) {
        this.vertx = vertx;
        this.cClOptns = cClOptns;
        active = true;
    }

    /**
     * @return given instance of cache manager.
     */
    static CacheManager getInstance() {
        checkIfActive();
        return instance;
    }

    /**
     * @param name - cache's name
     * @return fully initialized map cache.
     */
    <K, V> Map<K, V> createAndGetCacheMap(String name) {
        checkIfActive();
        return createAndGetCacheMap(name, true, Optional.empty());
    }

    /**
     * @param name           - cache's name
     * @param enableDecoding - cache's entries have to decoded prior to be placed into the cache.
     * @return fully initialized map cache.
     */
    <K, V> Map<K, V> createAndGetCacheMap(String name, boolean enableDecoding) {
        checkIfActive();
        return createAndGetCacheMap(name, enableDecoding, Optional.empty());
    }

    /**
     * @param name           - cache's name
     * @param enableDecoding - cache's entries have to decoded prior to be placed into the cache.
     * @param map            - map's entries to be put directly put into the cache.
     * @return fully initialized map cache.
     */
    <K, V> Map<K, V> createAndGetCacheMap(String name, boolean enableDecoding, Optional<Map<K, V>> map) {
        checkIfActive();
        return (Map<K, V>) caches.computeIfAbsent(name, key -> new CacheMap<>(name, enableDecoding, createAndGetMapWatch(name), map));
    }


    // LEGACY - should be removed
    Watch<ServiceList> createAndGetNodeWatch() {
        checkIfActive();
        Watch<ServiceList> serviceWatch = Watch.services(vertx, cClOptns);
        watches.add(serviceWatch);
        return serviceWatch;
    }

    /**
     * Creates consul (kv store specific) watch.
     */
    private Watch<KeyValueList> createAndGetMapWatch(String mapName) {
        Watch<KeyValueList> kvWatch = Watch.keyPrefix(mapName, vertx, cClOptns);
        watches.add(kvWatch);
        return kvWatch;
    }

    /**
     * Checks whether given cache manager is active.
     *
     * @throws VertxException if cache manager is not active.
     */
    private static void checkIfActive() {
        if (!active) {
            throw new VertxException("Cache manager is not active.");
        }
    }
}
