package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueList;
import io.vertx.ext.consul.Watch;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Distributed map implementation based on consul key-value store.
 * <p>
 * Note: Since it is a sync - based map (i.e blocking) - run potentially "blocking code" out of vertx-event-loop context.
 * TODO: 1) most of logging has to be removed when consul cluster manager is more or less stable.
 * TODO: 2) everything has to be documented in javadocs.
 *
 * @author Roman Levytskyi
 */
public final class ConsulSyncMap<K, V> extends ConsulAbstractMap<K, V> implements Map<K, V> {

    private final static Logger log = LoggerFactory.getLogger(ConsulSyncMap.class);

    private final String name;
    private final Vertx vertx;
    private final ConsulClient consulClient;

    // internal cache of consul KV store.
    private Map<K, V> cache;

    public ConsulSyncMap(String name, Vertx vertx, ConsulClient consulClient) {
        Objects.requireNonNull(vertx);
        Objects.requireNonNull(consulClient);
        this.name = name;
        this.vertx = vertx;
        this.consulClient = consulClient;
        initCache();
        registerWatcher();
        printCache();
    }

    /**
     * note: blocking call.
     */
    private void initCache() {
        log.trace("Initializing haInfo internal cache ... ");
        CompletableFuture<Map<K, V>> completableFuture = new CompletableFuture<>();
        consulClient.getValues(name, futureMap -> {
            if (futureMap.succeeded()) {
                if (futureMap.result().getList() == null) {
                    completableFuture.complete(new ConcurrentHashMap<>());
                } else {
                    Map<K, V> collectedMap = futureMap.result().getList()
                            .stream()
                            .collect(Collectors.toMap(keyValue -> (K) keyValue.getKey(), keyValue -> {
                                try {
                                    return decode(keyValue.getValue());
                                } catch (Exception e) {
                                    throw new VertxException(e);
                                }
                            }));
                    completableFuture.complete(collectedMap);
                }
            } else {
                completableFuture.completeExceptionally(futureMap.cause());
            }
        });

        try {
            if (cache == null) {
                cache = new ConcurrentHashMap<>();
            }
            cache.putAll(completableFuture.get());
            log.trace("__vertx.haInfo cache has been initialized: '{}'", Json.encodePrettily(cache));
        } catch (InterruptedException | ExecutionException e) {
            throw new VertxException(e);
        }
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
        String consulKey = getConsulKey(name, key);
        try {
            consulClient.putValue(consulKey, encode(value), event -> {
                if (event.succeeded()) {
                    log.trace("KV: '{}' -> '{}' has been placed to Consul KV store.", consulKey, value);
                } else {
                    log.warn("Can't put KV: '{}' -> '{}' to Consul KV store. Details: '{}'", consulKey, value, event.cause().toString());
                }
            });

        } catch (Exception e) {
            throw new VertxException(e);
        }
        return cache.put(key, value);
    }

    @Override
    public V remove(Object key) {
        String consulKey = name + "/" + key.toString();
        consulClient.deleteValue(consulKey, event -> {
            if (event.succeeded()) {
                log.trace("K: '{}' record has been deleted from Consul KV Store", consulKey);
            } else {
                log.warn("Can't delete a record by K: '{}' from Consul store due to: '{}'", consulKey, event.cause().toString());
            }
        });
        return cache.remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        log.trace("Putting: '{}' into Consul KV store.", Json.encodePrettily(m));

        m.forEach((key, value) -> {
            try {
                consulClient.putValue(getConsulKey(name, key), encode(value), event -> {
                });
            } catch (Exception e) {
                throw new VertxException(e);
            }

        });
        cache.putAll(m);
    }

    @Override
    public void clear() {
        log.trace("Clearing the KV store name by key prefix: '{}'", this.name);

        consulClient.deleteValues(this.name, event -> {
            if (event.succeeded()) {
                log.trace("Consul KV store has been cleared by key prefix: '{}'", name);
            } else {
                log.error("Can't clear Consul KV store by key prefix: '{}' due to: '{}'", name, event.cause().toString());
            }
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
                .keyPrefix(name, vertx)
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
                                    String extractedKey = keyValue.getKey().replace(name + "/", "");
                                    log.trace("Adding the KV: '{}' -> '{}' to local cache.", extractedKey, keyValue.getValue());
                                    try {
                                        cache.put((K) extractedKey, decode(keyValue.getValue()));
                                    } catch (Exception e) {
                                        throw new VertxException(e);
                                    }
                                });
                            } else if ((Objects.isNull(nextKvList) || Objects.isNull(nextKvList.getList()))) {
                                log.trace("Clearing all cache since Consul KV store seems to be empty.");
                                cache.clear();
                            } else if (Objects.nonNull(prevKvList.getList()) && Objects.nonNull(nextKvList.getList())) {
                                if (nextKvList.getList().size() == prevKvList.getList().size()) {
                                    nextKvList.getList().forEach(keyValue -> {
                                        String extractedKey = keyValue.getKey().replace(name + "/", "");
                                        log.trace("Updating the KV: '{}' -> '{}' to local cache.", extractedKey, keyValue.getValue());
                                        try {
                                            cache.put((K) extractedKey, decode(keyValue.getValue()));
                                        } catch (Exception e) {
                                            throw new VertxException(e);
                                        }
                                    });
                                } else if (nextKvList.getList().size() > prevKvList.getList().size()) {
                                    Set<KeyValue> nextS = new HashSet<>(nextKvList.getList());
                                    nextS.removeAll(new HashSet<>(prevKvList.getList()));
                                    if (!nextS.isEmpty()) {
                                        nextS.forEach(keyValue -> {
                                            String extractedKey = keyValue.getKey().replace(name + "/", "");
                                            log.trace("Adding the KV: '{}' -> '{}' to local cache.", extractedKey, keyValue.getValue());
                                            try {
                                                cache.put((K) extractedKey, decode(keyValue.getValue()));
                                            } catch (Exception e) {
                                                throw new VertxException(e);
                                            }
                                        });
                                    }
                                } else {
                                    Set<KeyValue> prevS = new HashSet<>(prevKvList.getList());
                                    prevS.removeAll(new HashSet<>(nextKvList.getList()));
                                    if (!prevS.isEmpty()) {
                                        prevS.forEach(keyValue -> {
                                            String extractedKey = keyValue.getKey().replace(name + "/", "");
                                            log.trace("Removing the KV: '{}' -> '{}' from local cache.", extractedKey, keyValue.getValue());
                                            try {
                                                cache.put((K) extractedKey, decode(keyValue.getValue()));
                                            } catch (Exception e) {
                                                throw new VertxException(e);
                                            }
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
    // TODO : removed it when consul cluster manager is more or less stable.
    private void printCache() {
        vertx.setPeriodic(10000, handler -> log.trace("Internal cache: '{}'", Json.encodePrettily(cache)));
    }
}
