package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.consul.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

import static io.vertx.spi.cluster.consul.impl.Utils.decode;

/**
 * Distributed sync map implementation based on consul key-value store.
 * <p>
 * Given implementation fully relies on internal cache (consul client doesn't have any cache built-in so we are sort of forced to come up with something custom)
 * which essentially is {@link java.util.concurrent.ConcurrentHashMap}.
 * Now:
 * - cache read operations happens synchronously by simple reading from {@link java.util.concurrent.ConcurrentHashMap}.
 * - cache write operations happens through consul watch that watches consul kv store updates
 * (that's the only way we can apply to acknowledge successful write operation to consul kv store).
 * <p>
 * Note: given cache implementation MIGHT NOT BE mature enough to handle different sort of failures that might occur (hey, that's distributed systems world :))
 *
 * @author Roman Levytskyi
 */
public final class ConsulSyncMap<K, V> extends ConsulMap<K, V> implements Map<K, V> {

    private final static Logger log = LoggerFactory.getLogger(ConsulSyncMap.class);

    private final Map<K, V> cache;
    private final Vertx vertx;
    private final ConsulClientOptions consulClientOptions;

    public ConsulSyncMap(String name,
                         Vertx vertx,
                         ConsulClient consulClient,
                         ConsulClientOptions consulClientOptions,
                         String sessionId,
                         Map<K, V> cache) {
        super(consulClient, name, sessionId);
        this.vertx = vertx;
        this.consulClientOptions = consulClientOptions;
        this.cache = cache;
        watchHaInfoMap().start();
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
        putValue(key, value);
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
     * Watches registration. Watch has to be registered in order to keep the internal cache consistent and in sync with central
     * Consul KV store. Watch listens to events that are coming from Consul KV store and updates the internal cache appropriately.
     */
    private Watch watchHaInfoMap() {
        return Watch.keyPrefix(name, vertx, consulClientOptions).setHandler(promise -> {
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
        });
    }

    // just a dummy helper method [it's gonna get removed] to print out every 5 sec the data that resides within the internal cache.
    // TODO : removed it when consul cluster manager is more or less stable.
    private void printCache() {
        vertx.setPeriodic(10000, handler -> log.trace("Internal HaInfo (Sync) Map: '{}'", Json.encodePrettily(cache)));
    }
}
