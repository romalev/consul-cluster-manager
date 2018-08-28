package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.ext.consul.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static io.vertx.spi.cluster.consul.impl.ClusterSerializationUtils.decode;

/**
 * Distributed async map implementation based on consul key-value store.
 * <p>
 * Note: given map is used in vertx shared data - it is used by vertx nodes to share the data, entries of this map are always PERSISTENT not EPHEMERAL.
 *
 * @author Roman Levytskyi
 */
public class ConsulAsyncMap<K, V> extends ConsulMap<K, V> implements AsyncMap<K, V> {

    private static final Logger log = LoggerFactory.getLogger(ConsulAsyncMap.class);

    private final Vertx vertx;
    private final Map<K, V> cache;

    public ConsulAsyncMap(String name, Vertx vertx, ConsulClient cC, Watch<KeyValueList> watch) {
        super(name, cC);
        this.vertx = vertx;
        cache = new ConsulVolatileCache<>(name, true, watch, Optional.empty());
        printOutAsyncMap();
    }

    @Override
    public void get(K k, Handler<AsyncResult<V>> asyncResultHandler) {
        getValue(k).setHandler(asyncResultHandler);
    }

    @Override
    public void put(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
        putValue(k, v)
                .compose(aBoolean -> aBoolean ? Future.succeededFuture() : Future.<Void>failedFuture(k.toString() + "wasn't put to " + name))
                .setHandler(completionHandler);
    }

    @Override
    public void put(K k, V v, long ttl, Handler<AsyncResult<Void>> completionHandler) {
        assertKeyAndValueAreNotNull(k, v)
                .compose(aVoid -> getTtlSessionId(ttl, k))
                .compose(id -> putValue(k, v, new KeyValueOptions().setAcquireSession(id)))
                .compose(aBoolean -> aBoolean ? Future.succeededFuture() : Future.<Void>failedFuture(k.toString() + "wasn't put to " + name))
                .setHandler(completionHandler);
    }

    @Override
    public void putIfAbsent(K k, V v, Handler<AsyncResult<V>> completionHandler) {
        log.trace("'{}' - putting if absent KV: '{}' -> '{}' to CKV.", name, k, v);
        putIfAbsent(k, v, Optional.empty()).setHandler(completionHandler);
    }

    @Override
    public void putIfAbsent(K k, V v, long ttl, Handler<AsyncResult<V>> completionHandler) {
        log.trace("'{}' - putting if absent KV: '{}' -> '{}' to CKV with ttl", name, k, v, ttl);
        assertKeyAndValueAreNotNull(k, v)
                .compose(aVoid -> getTtlSessionId(ttl, k))
                .compose(sessionId -> putIfAbsent(k, v, Optional.of(sessionId)))
                .setHandler(completionHandler);
    }

    @Override
    public void remove(K k, Handler<AsyncResult<V>> asyncResultHandler) {
        removeValue(k).setHandler(asyncResultHandler);
    }

    @Override
    public void removeIfPresent(K k, V v, Handler<AsyncResult<Boolean>> resultHandler) {
        // removes a value from the map, only if entry already exists with same value.
        log.trace("'{}' - removing if present an entry by KV: '{}' -> '{}' from CKV.", name, k, v);
        getValue(k)
                .compose(value -> {
                    Future<Boolean> future = Future.future();
                    if (v.equals(value)) {
                        consulClient.deleteValue(getConsulKey(name, k), res -> {
                            if (res.succeeded()) future.complete(true);
                            else future.fail(res.cause());
                        });
                    } else {
                        future.complete(false);
                    }
                    return future;
                })
                .setHandler(resultHandler);
    }

    @Override
    public void replace(K k, V v, Handler<AsyncResult<V>> asyncResultHandler) {
        // replaces the entry only if it is currently mapped to some value.
        log.trace("'{}' - replacing an entry with K: '{}' by new V: '{}'.", name, k, v);
        getValue(k)
                .compose(curV -> {
                    Future<V> newValueFuture = Future.future();
                    if (curV == null) {
                        newValueFuture.complete();
                    } else {
                        put(k, v, event -> {
                            if (event.succeeded()) newValueFuture.complete(v);
                            else newValueFuture.fail(event.cause());
                        });
                    }
                    return newValueFuture;
                })
                .setHandler(asyncResultHandler);
    }

    @Override
    public void replaceIfPresent(K k, V oldValue, V newValue, Handler<AsyncResult<Boolean>> resultHandler) {
        // replaces the entry only if it is currently mapped to a specific value.
        assertValueIsNotNull(newValue)
                .compose(aVoid -> assertValueIsNotNull(oldValue))
                .compose(aVoid -> getValue(k))
                .compose(curV -> {
                    Future<Boolean> future = Future.future();
                    if (curV != null) {
                        if (curV.equals(oldValue)) {
                            put(k, newValue, resultPutHandler -> {
                                if (resultPutHandler.succeeded()) {
                                    log.trace("Old V: '{}' has been replaced by new V: '{}' where K: '{}'", oldValue, newValue, k);
                                    future.complete(true);
                                } else {
                                    log.trace("Can't replace old V: '{}' by new V: '{}' where K: '{}' due to: '{}'", oldValue, newValue, k, resultPutHandler.cause().toString());
                                    future.fail(resultPutHandler.cause());
                                }
                            });
                        } else {
                            log.trace("An entry with K: '{}' doesn't map to old V: '{}' so it won't get replaced.", k, oldValue);
                            future.complete(false);
                        }
                    } else {
                        log.trace("An entry with K: '{}' doesn't exist ");
                        future.complete(false);
                    }
                    return future;
                })
                .setHandler(resultHandler);
    }

    @Override
    public void clear(Handler<AsyncResult<Void>> resultHandler) {
        clearUp().setHandler(resultHandler);
    }

    @Override
    public void size(Handler<AsyncResult<Integer>> resultHandler) {
        log.trace("Calculating the size of: {}", name);
        consulKeys().compose(list -> Future.succeededFuture(list.size())).setHandler(resultHandler);
    }

    @Override
    public void keys(Handler<AsyncResult<Set<K>>> asyncResultHandler) {
        log.trace("Fetching all keys from: {}", name);
        consulKeys().compose(list -> Future.succeededFuture(list.stream().map(s -> (K) s).collect(Collectors.toSet()))).setHandler(asyncResultHandler);
    }

    @Override
    public void values(Handler<AsyncResult<List<V>>> asyncResultHandler) {
        log.trace("Fetching all values from: {}", name);
        consulEntries()
                .compose(keyValueList -> {
                    Future<List<V>> future = Future.future();
                    List<KeyValue> list = getListResult(keyValueList);
                    List<V> values = new ArrayList<>();
                    list.forEach(keyValue -> {
                        try {
                            V value = decode(keyValue.getValue());
                            values.add(value);
                        } catch (Exception e) {
                            log.error("Exception occurred while decoding value: '{}' due to: '{}'", keyValue.getValue(), e.getCause().toString());
                        }
                    });
                    log.trace("Vs: '{}' of: '{}'", values, name);
                    future.complete(values);
                    return future;
                })
                .setHandler(asyncResultHandler);
    }

    @Override
    public void entries(Handler<AsyncResult<Map<K, V>>> asyncResultHandler) {
        log.trace("Fetching all entries from: {}", name);
        // gets the entries of the map, asynchronously.
        consulEntries()
                .compose(keyValueList -> {
                    Future<Map<K, V>> future = Future.future();
                    List<KeyValue> list = getListResult(keyValueList);
                    Map<K, V> resultMap = new ConcurrentHashMap<>();
                    list.forEach(keyValue -> {
                        try {
                            K key = (K) keyValue.getKey();
                            V value = decode(keyValue.getValue());
                            resultMap.put(key, value);
                        } catch (Exception e) {
                            log.error("Exception occurred while decoding value: '{}' due to: '{}'", keyValue.getValue(), e.getCause().toString());
                        }
                    });
                    log.trace("Listing all entries within async KV store: '{}' is: '{}'", name, Json.encodePrettily(resultMap));
                    future.complete(resultMap);
                    return future;
                })
                .setHandler(asyncResultHandler);
    }

    /**
     * Puts the entry only if there is no entry with the key already present. If key already present then the existing
     * value will be returned to the handler, otherwise null.
     *
     * @param k         - holds the entry's key.
     * @param v         - holds the entry's value.
     * @param sessionId - holds the ttl session id.
     * @return future existing value if k is already present, otherwise future null.
     */
    private Future<V> putIfAbsent(K k, V v, Optional<String> sessionId) {
        // TODO: verify if this works.
        KeyValueOptions casOpts = new KeyValueOptions();
        sessionId.ifPresent(casOpts::setAcquireSession);
        return putValue(k, v, casOpts)
                .compose(aBoolean -> {
                    Future<V> future = Future.succeededFuture();
                    if (aBoolean) {
                        future.complete();
                    } else {
                        // key already present
                        get(k, future.completer());
                    }
                    return future;
                });
    }

    // helper method used to print out periodically the async consul map.
    private void printOutAsyncMap() {
        vertx.setPeriodic(15000, event -> entries(Future.future()));
    }
}
