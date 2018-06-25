package io.vertx.spi.cluster.consul.impl;

import io.reactivex.Observable;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueList;
import io.vertx.ext.consul.Watch;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.consul.ConsulClient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Distributed map implementation based on consul key-value store.
 * <p>
 * Note: Since it is a sync - based map (i.e blocking) - run potentially "blocking code" out of vertx-event-loop context.
 * TODO: 1) most of logging has to be removed when consul cluster manager is more or less stable.
 * TODO: 2) everything has to be documented in javadocs.
 *
 * @author Roman Levytskyi
 */
public final class ConsulSyncMap<K, V> implements Map<K, V> {

    private final static Logger log = LoggerFactory.getLogger(ConsulSyncMap.class);
    private static final String CLUSTER_MAP_NAME = "__vertx.haInfo";

    // thread - sage map instance.
    private static volatile ConsulSyncMap instance;
    // used to lock the map creation operation.
    private static Object lock = new Object();

    private final String name;
    private final Vertx rxVertx;
    private final ConsulClient consulClient;

    // internal cache of consul KV store.
    private Map<K, V> cache;

    public ConsulSyncMap(Vertx rxVertx, ConsulClient consulClient) {
        Objects.requireNonNull(rxVertx);
        Objects.requireNonNull(consulClient);
        this.name = CLUSTER_MAP_NAME;
        this.rxVertx = rxVertx;
        this.consulClient = consulClient;
        initCache();
        registerWatcher();
        // TODO : removed it when consul cluster manager is more or less stable.
        printCache();
    }

    private void initCache() {
        log.trace("Initializing ConsulSyncMap internal cache ... ");
        this.cache = new ConcurrentHashMap<>();
        Map<K, V> consulMap = consulClient
                .rxGetValues(name)
                .toObservable()
                .filter(keyValueList -> Objects.nonNull(keyValueList.getList()))
                .flatMapIterable(KeyValueList::getList)
                .toMap(keyValue -> ((K) keyValue.getKey().replace(CLUSTER_MAP_NAME + "/", "")), keyValue -> ((V) keyValue.getValue()))
                .blockingGet();

        cache.putAll(consulMap);
        log.trace("Internal cache has been initialized: '{}'", Json.encodePrettily(cache));
    }

    // !!! --- so far just really dummy impl. !!! --- //

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
        log.trace("Putting KV: '{}' -> '{}' to Consul KV store.", key, value);
        // async
        consulClient.rxPutValue(name + "/" + key.toString(), value.toString())
                .subscribe(
                        res -> log.trace("KV: '{}' -> '{}' has been placed to Consul KV store.", key, value),
                        throwable -> log.warn("Can't put KV: '{}' -> '{}' to Consul KV store. Details: '{}'", key, value, throwable.getMessage()));
        return cache.put(key, value);
    }

    @Override
    public V remove(Object key) {
        consulClient.rxDeleteValue(name + "/" + key.toString())
                .subscribe(
                        () -> log.trace("K: '{}' record has been deleted from Consul KV Store", key),
                        throwable -> log.warn("Can't delete a record by K: '{}' from Consul store. Details: '{}'", key, throwable.toString()));
        return cache.remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        log.trace("Putting: '{}' into Consul KV store.", Json.encodePrettily(m));
        Observable
                .fromIterable(m.entrySet())
                .flatMapSingle(entry -> consulClient.rxPutValue(name + "/" + entry.getKey().toString(), entry.getValue().toString()))
                .subscribe();
        cache.putAll(m);
    }

    @Override
    public void clear() {
        log.trace("Clearing the KV store name by key prefix: '{}'", CLUSTER_MAP_NAME);
        consulClient.rxDeleteValues(CLUSTER_MAP_NAME).subscribe(() -> {
            log.trace("Consul KV store has been cleared by key prefix: '{}'", CLUSTER_MAP_NAME);
        });
        cache.clear();
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
     * Watch registration. Watch has to be registered in order to keep the internal cache consistent and in sync with central
     * Consul KV store. Watch listens to events that are coming from Consul KV store and updates the internal cache appropriately.
     */
    private void registerWatcher() {
        Watch
                .keyPrefix(CLUSTER_MAP_NAME, rxVertx.getDelegate())
                .setHandler(promise -> {
                    if (promise.succeeded()) {
                        // We still need some sort of level synchronization on the internal cache since this is the entry point where the cache gets updated and in sync with Consul KV store.
                        // TODO : is it correct to do so ? Investigate moore deeply how concurrent hash map handles concurrent writes
                        synchronized (this) {
                            log.trace("Watch for Consul KV store has been registered.");
                            // TODO : more its are required to test this behaviour.
                            KeyValueList prevKvList = promise.prevResult();
                            KeyValueList nextKvList = promise.nextResult();

                            if (Objects.isNull(prevKvList) && Objects.nonNull(nextKvList) && Objects.nonNull(nextKvList.getList())) {
                                nextKvList.getList().forEach(keyValue -> {
                                    String extractedKey = keyValue.getKey().replace(CLUSTER_MAP_NAME + "/", "");
                                    log.trace("Adding the KV: '{}' -> '{}' to local cache.", extractedKey, keyValue.getValue());
                                    cache.put((K) extractedKey, (V) keyValue.getValue());
                                });
                            } else if ((Objects.isNull(nextKvList) || Objects.isNull(nextKvList.getList()))) {
                                log.trace("Clearing all cache since Consul KV store seems to be empty.");
                                cache.clear();
                            } else if (Objects.nonNull(prevKvList.getList()) && Objects.nonNull(nextKvList.getList())) {
                                if (nextKvList.getList().size() == prevKvList.getList().size()) {
                                    nextKvList.getList().forEach(keyValue -> {
                                        String extractedKey = keyValue.getKey().replace(CLUSTER_MAP_NAME + "/", "");
                                        log.trace("Updating the KV: '{}' -> '{}' to local cache.", extractedKey, keyValue.getValue());
                                        cache.put((K) extractedKey, (V) keyValue.getValue());
                                    });
                                } else if (nextKvList.getList().size() > prevKvList.getList().size()) {
                                    Set<KeyValue> nextS = new HashSet<>(nextKvList.getList());
                                    nextS.removeAll(new HashSet<>(prevKvList.getList()));
                                    if (!nextS.isEmpty()) {
                                        nextS.forEach(keyValue -> {
                                            String extractedKey = keyValue.getKey().replace(CLUSTER_MAP_NAME + "/", "");
                                            log.trace("Adding the KV: '{}' -> '{}' to local cache.", extractedKey, keyValue.getValue());
                                            cache.put((K) extractedKey, (V) keyValue.getValue());
                                        });
                                    }
                                } else {
                                    Set<KeyValue> prevS = new HashSet<>(prevKvList.getList());
                                    prevS.removeAll(new HashSet<>(nextKvList.getList()));
                                    if (!prevS.isEmpty()) {
                                        prevS.forEach(keyValue -> {
                                            String extractedKey = keyValue.getKey().replace(CLUSTER_MAP_NAME + "/", "");
                                            log.trace("Removing the KV: '{}' -> '{}' from local cache.", extractedKey, keyValue.getValue());
                                            cache.put((K) extractedKey, (V) keyValue.getValue());
                                            cache.remove(extractedKey);
                                        });
                                    }
                                }
                            }
                        }
                    } else {
                        log.error("Failed to register a watch. Details: '{}'", promise.cause().getMessage());
                    }
                })
                .start();
    }

    // just a dummy helper method [it's gonna get removed] to print out every 5 sec the data that resides within the internal cache.
    private void printCache() {
        rxVertx.setPeriodic(5000, handler -> log.trace("Internal cache: '{}'", Json.encodePrettily(cache)));
    }
}
