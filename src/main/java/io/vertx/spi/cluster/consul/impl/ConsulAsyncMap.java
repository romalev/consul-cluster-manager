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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Distributed async map implementation based on consul key-value store.
 * <p>
 * TODO: 1) most of logging has to be removed when consul cluster manager is more or less stable.
 * TODO: 2) everything has to be documented in javadocs.
 * TODO: 3) Marshalling and unmarshalling.
 * TODO: 4) Some caching perhaps ???
 *
 * @author Roman Levytskyi
 */
public class ConsulAsyncMap<K, V> extends ConsulMap<K, V> implements AsyncMap<K, V> {

    private static final Logger log = LoggerFactory.getLogger(ConsulAsyncMap.class);

    private final Vertx vertx;

    public ConsulAsyncMap(String name,
                          Vertx vertx,
                          ConsulClient consulClient,
                          ConsulClientOptions consulClientOptions,
                          String sessionId) {
        super(consulClient, name, sessionId);
        this.vertx = vertx;
        printOutAsyncMap();
    }

    @Override
    public void get(K k, Handler<AsyncResult<V>> asyncResultHandler) {
        getValue(k).setHandler(asyncResultHandler);
    }

    @Override
    public void put(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
        putValue(k, v).setHandler(completionHandler);
    }

    @Override
    public void put(K k, V v, long ttl, Handler<AsyncResult<Void>> completionHandler) {
        getTtlSessionHandler(ttl)
                .compose(id -> putValue(k, v, new KeyValueOptions().setAcquireSession(id)))
                .setHandler(completionHandler);
    }

    @Override
    public void putIfAbsent(K k, V v, Handler<AsyncResult<V>> completionHandler) {
        log.trace("'{}' - putting if absent KV: '{}' -> '{}' to CKV.", name, k, v);
        putIfAbsent(k, v, defaultKvOptions).setHandler(completionHandler);
    }

    @Override
    public void putIfAbsent(K k, V v, long ttl, Handler<AsyncResult<V>> completionHandler) {
        log.trace("'{}' - putting if absent KV: '{}' -> '{}' to CKV with ttl", name, k, v, ttl);
        getTtlSessionHandler(ttl)
                .compose(sessionId -> putIfAbsent(k, v, new KeyValueOptions().setAcquireSession(sessionId)))
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
                        remove(k, event -> {
                            if (event.succeeded()) future.complete(true);
                            else future.fail(event.cause());
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
                .compose(receivedValue -> {
                    Future<V> newValueFuture = Future.future();
                    if (receivedValue != null) {
                        put(k, receivedValue, event -> {
                            if (event.succeeded()) newValueFuture.complete(receivedValue);
                            else newValueFuture.fail(event.cause());
                        });
                    } else {
                        newValueFuture.complete();
                    }
                    return newValueFuture;
                })
                .setHandler(asyncResultHandler);
    }

    @Override
    public void replaceIfPresent(K k, V oldValue, V newValue, Handler<AsyncResult<Boolean>> resultHandler) {
        // replaces the entry only if it is currently mapped to a specific value.
        assertValueIsNotNull(oldValue)
                .compose(aVoid -> getValue(k))
                .compose(value -> {
                    Future<Boolean> future = Future.future();
                    if (Objects.nonNull(value)) {
                        if (value.equals(oldValue)) {
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
                            log.trace("An entry with K: '{}' doesn't map to old V: '{}' so it wo'nt get replaced.", k, oldValue);
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
        log.trace("Calculating the size of: {}", this.name);
        Future<Integer> future = Future.future();

        consulClient.getKeys(name, resultSizeHandler -> {
            if (resultSizeHandler.succeeded()) {
                log.trace("Size of: '{}' is: '{}'", this.name, resultSizeHandler.result().size());
                future.complete(resultSizeHandler.result().size());
            } else {
                log.error("Error occurred while calculating the size of : '{}' due to: '{}'", this.name, resultSizeHandler.cause().toString());
                future.fail(resultSizeHandler.cause());
            }
        });
        future.setHandler(resultHandler);
    }

    @Override
    public void keys(Handler<AsyncResult<Set<K>>> asyncResultHandler) {
        log.trace("Fetching all keys from: {}", name);
        Future<Set<K>> future = Future.future();
        consulClient.getKeys(name, resultHandler -> {
            if (resultHandler.succeeded()) {
                List<String> keys = getListResult(resultHandler.result());
                log.trace("Ks: '{}' of: '{}'", keys, name);
                future.complete(new HashSet<>(keys.stream().map(s -> (K) (s)).collect(Collectors.toList())));
            } else {
                log.error("Error occurred while fetching all the keys from: '{}' due to: '{}'", this.name, resultHandler.cause().toString());
                future.fail(resultHandler.cause());
            }
        });
        future.setHandler(asyncResultHandler);
    }

    @Override
    public void values(Handler<AsyncResult<List<V>>> asyncResultHandler) {
        log.trace("Fetching all values from: {}", this.name);
        Future<List<V>> future = Future.future();
        consulClient.getValues(name, resultHandler -> {
            if (resultHandler.succeeded()) {
                List<KeyValue> list = getListResult(resultHandler.result());
                List<V> values = new ArrayList<>();
                list.forEach(keyValue -> {
                    try {
                        V value = Utils.decode(keyValue.getValue());
                        values.add(value);
                    } catch (Exception e) {
                        log.error("Exception occurred while decoding value: '{}' due to: '{}'", keyValue.getValue(), e.getCause().toString());
                    }
                });
                log.trace("Vs: '{}' of: '{}'", values, name);
                future.complete(values);
            } else {
                log.error("Error occurred while fetching all the values from: '{}' due to: '{}'", this.name, resultHandler.cause().toString());
                future.fail(resultHandler.cause());
            }
        });
        future.setHandler(asyncResultHandler);
    }

    @Override
    public void entries(Handler<AsyncResult<Map<K, V>>> asyncResultHandler) {
        log.trace("Fetching all entries from: {}", this.name);
        // gets the entries of the map, asynchronously.
        Future<Map<K, V>> future = Future.future();
        consulClient.getValues(name, resultHandler -> {
            if (resultHandler.succeeded()) {
                List<KeyValue> list = getListResult(resultHandler.result());
                Map<K, V> resultMap = new ConcurrentHashMap<>();
                list.forEach(keyValue -> {
                    try {
                        K key = (K) keyValue.getKey();
                        V value = Utils.decode(keyValue.getValue());
                        resultMap.put(key, value);
                    } catch (Exception e) {
                        log.error("Exception occurred while decoding value: '{}' due to: '{}'", keyValue.getValue(), e.getCause().toString());
                    }
                });
                log.trace("Listing all entries within async KV store: '{}' is: '{}'", name, Json.encodePrettily(resultHandler));
                future.complete(resultMap);
            } else {
                log.error("Failed to fetch all entries of async map '{}' due to : '{}'", name, resultHandler.cause().toString());
                future.fail(resultHandler.cause());
            }
        });
        future.setHandler(asyncResultHandler);
    }

    /**
     * Puts the entry only if there is no entry with the key already present. If key already present then the existing
     * value will be returned to the handler, otherwise null.
     *
     * @param k               - holds the entry's key.
     * @param v               - holds the entry's value.
     * @param keyValueOptions - holds kv consul options.
     * @return future existing value if k is already present, otherwise future null.
     */
    private Future<V> putIfAbsent(K k, V v, KeyValueOptions keyValueOptions) {
        return getValue(k)
                .compose(value -> {
                    Future<V> future = Future.future();
                    if (value == null) {
                        putValue(k, v, keyValueOptions).setHandler(event -> {
                            if (event.succeeded()) future.complete();
                            else future.fail(event.cause());
                        });
                    } else {
                        future.complete(v);
                    }
                    return future;
                });
    }

    /**
     * Creates TTL dedicated consul session. TTL on entries is handled by relaying on consul session itself.
     * We have to register the session first it consul and then bound the session's id with entries we want to put ttl on.
     *
     * @param ttl - holds ttl in ms, this value must be between {@code 10s} and {@code 86400s} currently.
     * @return session id.
     */
    private Future<String> getTtlSessionHandler(long ttl) {
        if (ttl < 10000) {
            log.warn("Specified ttl is less than allowed in consul -> min ttl is 10s.");
            ttl = 10000;
        }

        if (ttl > 86400000) {
            log.warn("Specified ttl is more that allowed in consul -> max ttl is 86400s.");
            ttl = 86400000;
        }

        String sessionName = "ttlSession_" + UUID.randomUUID().toString();
        Future<String> future = Future.future();
        SessionOptions ttlSession = new SessionOptions()
                .setTtl(TimeUnit.MILLISECONDS.toSeconds(ttl))
                .setBehavior(SessionBehavior.DELETE)
                .setName(sessionName);

        consulClient.createSessionWithOptions(ttlSession, idHandler -> {
            if (idHandler.succeeded()) {
                log.trace("TTL session has been created with id: '{}'", idHandler.result());
                future.complete(idHandler.result());
            } else {
                log.error("Failed to created ttl consul session due to: '{}'", idHandler.cause().toString());
                future.fail(idHandler.cause());
            }
        });
        return future;
    }

    private <T> List<T> getListResult(List<T> list) {
        return list == null ? Collections.emptyList() : list;
    }

    private List<KeyValue> getListResult(KeyValueList keyValueList) {
        return keyValueList == null || keyValueList.getList() == null ? Collections.emptyList() : keyValueList.getList();
    }

    // helper method used to print out periodically the async consul map.
    private void printOutAsyncMap() {
        vertx.setPeriodic(5000, event -> entries(Future.future()));
    }
}
