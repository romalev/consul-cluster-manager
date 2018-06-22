package io.vertx.spi.cluster.consul.impl;

import io.reactivex.Observable;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueList;
import io.vertx.ext.consul.Watch;
import io.vertx.reactivex.RxHelper;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.consul.ConsulClient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
public final class ConsulSyncMap implements Map<String, String> {

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
    private Map<String, String> cache;

    // thread - safe.
    public static ConsulSyncMap getInstance(final Vertx rxVertx, final ConsulClient consulClient) {
        ConsulSyncMap result = instance;
        if (Objects.isNull(result)) {
            synchronized (lock) {
                result = instance;
                if (Objects.isNull(result)) {
                    instance = result = new ConsulSyncMap(rxVertx, consulClient);
                    log.trace("Instance of '{}' has been successfully created.", ConsulSyncMap.class.getSimpleName());
                }
            }
        }
        return result;
    }

    private ConsulSyncMap(Vertx rxVertx, ConsulClient consulClient) {
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
        Map<String, String> consulMap = consulClient
                .rxGetValues(name)
                .toObservable()
                .filter(keyValueList -> Objects.nonNull(keyValueList.getList()))
                .flatMapIterable(KeyValueList::getList)
                .toMap(KeyValue::getKey, KeyValue::getValue)
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
    public String get(Object key) {
        return cache.get(key);
    }

    @Nullable
    @Override
    public String put(String key, String value) {
        log.trace("Putting KV: '{}' -> '{}' to Consul KV store.", key, value);
        // async
        consulClient.rxPutValue(name + "/" + key, value)
                .subscribe(
                        res -> log.trace("KV: '{}' -> '{}' has been placed to Consul KV store.", key, value),
                        throwable -> log.trace("Can't put KV: '{}' -> '{}' to Consul KV store. Details: '{}'", key, value, throwable.getMessage()));
        return cache.put(key, value);
    }

    @Override
    public String remove(Object key) {
        consulClient.rxDeleteValue(name + "/" + key).subscribe();
        return cache.remove(key);
    }

    @Override
    public void putAll(@NotNull Map<? extends String, ? extends String> m) {
        log.trace("Putting: '{}' into Consul KV store.", Json.encodePrettily(m));
        Observable
                .fromIterable(m.entrySet())
                .flatMapSingle(entry -> consulClient.rxPutValue(entry.getKey(), entry.getValue()))
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
    public Set<String> keySet() {
        return cache.keySet();
    }

    @NotNull
    @Override
    public Collection<String> values() {
        return cache.values();
    }

    @NotNull
    @Override
    public Set<Entry<String, String>> entrySet() {
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
                        log.trace("Watch for Consul KV store has been registered.");
                        // We still need some sort of level synchronization on the internal cache since this is the entry point where the cache
                        // gets updated and in sync with Consul KV store.
                        // TODO : is it correct to do so ???
                        if (Objects.nonNull(promise.nextResult()) && Objects.nonNull(promise.nextResult().getList())) {
                            synchronized (this) {
                                Observable
                                        .fromIterable(promise.nextResult().getList())
                                        .subscribeOn(RxHelper.blockingScheduler(rxVertx.getDelegate()))
                                        // TODO proper filtering perhaps ???
                                        .doOnNext(keyValue -> {
                                            String extractedKey = keyValue.getKey().replace(CLUSTER_MAP_NAME + "/", "");
                                            log.trace("Watcher: updating the internal cache by: '{}' ->  '{}'", extractedKey, keyValue.getValue());
                                            cache.put(extractedKey, keyValue.getValue());
                                        })
                                        .subscribe();
                            }
                        }
                    } else {
                        log.error("Failed to register a watch. Details: '{}'", promise.cause().getMessage());
                        promise.failed();
                    }
                })
                .start();
    }

    // just a dummy helper method [it's gonna get removed] to print out every 5 sec the data that resides within the internal cache.
    private void printCache() {
        rxVertx.setPeriodic(5000, handler -> log.trace("Internal cache: '{}'", Json.encodePrettily(cache)));
    }
}
