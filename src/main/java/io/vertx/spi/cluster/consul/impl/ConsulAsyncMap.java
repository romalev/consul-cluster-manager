package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.ext.consul.KeyValue;

import java.util.*;
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

    public ConsulAsyncMap(String name,
                          Vertx vertx,
                          ConsulClient consulClient,
                          ConsulClientOptions consulClientOptions,
                          String sessionId) {
        super(vertx, consulClient, consulClientOptions, name, sessionId);
        printOutAsyncMap();
    }

    @Override
    public void get(K k, Handler<AsyncResult<V>> asyncResultHandler) {
        log.trace("Getting an entry by K: '{}' from Consul Async KV store.", k.toString());
        assertKeyIsNotNull(k)
                .compose(aVoid -> {
                    Future<V> future = Future.future();
                    final String consulKey = getConsulKey(this.name, k);
                    consulClient.getValue(consulKey, resultHandler -> {
                        if (resultHandler.succeeded()) {
                            if (Objects.nonNull(resultHandler.result()) && Objects.nonNull(resultHandler.result().getValue())) {
                                log.trace("Got an entry '{}' - '{}'", consulKey, resultHandler.result().getValue());
                                // FIXME : this is wrong.
                                future.complete((V) resultHandler.result().getValue());
                            } else {
                                future.complete();
                            }
                        } else {
                            log.error("Failed to get an entry by K: '{}' from Consul Async KV store. Details: '{}'", k.toString(), resultHandler.cause().toString());
                            future.fail(resultHandler.cause());
                        }
                    });
                    return future;
                }).setHandler(asyncResultHandler);
    }

    @Override
    public void put(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
        final String consulKey = getConsulKey(this.name, k);
        log.trace("Putting KV: '{}' -> '{}' to Consul Async KV store.", consulKey, v.toString());
        assertKeyAndValueAreNotNull(k, v)
                .compose(aVoid -> {
                    Future<Void> future = Future.future();
                    consulClient.putValue(consulKey, v.toString(), resultHandler -> {
                        if (resultHandler.succeeded()) {
                            log.trace("KV: '{}' -> '{}' has been put to Consul Async KV store.", consulKey, v.toString());
                            future.complete();
                        } else {
                            log.error("Failed to put KV: '{}' -> '{}' to Consul Async KV store. Details: '{}'", consulKey, v.toString(), resultHandler.cause().toString());
                            future.fail(resultHandler.cause());
                        }
                    });
                    return future;
                }).setHandler(completionHandler);
    }

    @Override
    public void put(K k, V v, long ttl, Handler<AsyncResult<Void>> completionHandler) {
        // TODO ttl
        put(k, v, completionHandler);
    }

    @Override
    public void putIfAbsent(K k, V v, Handler<AsyncResult<V>> completionHandler) {
        // puts the entry only if there is no entry with the key already present. If key already present then the existing
        // value will be returned to the handler, otherwise null.
        log.trace("Putting if absent KV: '{}' -> '{}' to Consul  Async KV store ", getConsulKey(this.name, k), v.toString());
        assertKeyAndValueAreNotNull(k, v)
                .compose(aVoid -> {
                    Future<V> future = Future.future();
                    get(k, future.completer());
                    return future;
                })
                .compose(vV -> {
                    Future<V> future = Future.future();
                    if (Objects.isNull(vV)) {
                        future.complete();
                    } else {
                        log.trace("KV: '{}' -> '{}' is already present in Consul Async KV store.", getConsulKey(this.name, k), vV.toString());
                        future.complete(vV);
                    }
                    return future;
                })
                .setHandler(completionHandler);
    }

    @Override
    public void putIfAbsent(K k, V v, long ttl, Handler<AsyncResult<V>> completionHandler) {
        // TODO ttl
        putIfAbsent(k, v, completionHandler);
    }

    @Override
    public void remove(K k, Handler<AsyncResult<V>> asyncResultHandler) {
        String consulKey = getConsulKey(this.name, k);
        log.trace("Removing an entry by K: '{}' from Consul KV Async store.", consulKey);
        assertKeyIsNotNull(k)
                .compose(aVoid -> {
                    Future<V> future = Future.future();
                    get(k, future.completer());
                    return future;
                })
                .compose(value -> {
                    Future<V> future = Future.future();
                    if (Objects.nonNull(value)) {
                        consulClient.deleteValue(consulKey, resultHandler -> {
                            if (resultHandler.succeeded()) {
                                log.trace("Entry with K: '{}' has been removed from Consul Async KV store.", consulKey);
                                future.complete(value);
                            } else {
                                log.error("Error occurred while trying to remove an entry by K: '{}' from Consul Async KV store. Details: '{}'", consulKey, resultHandler.cause().toString());
                                future.fail(resultHandler.cause());
                            }
                        });
                    } else {
                        log.trace("Can't find an entry with K: '{}' within Consul Async KV store.", consulKey);
                        future.complete();
                    }
                    return future;
                })
                .setHandler(asyncResultHandler);
    }

    @Override
    public void removeIfPresent(K k, V v, Handler<AsyncResult<Boolean>> resultHandler) {
        // removes a value from the map, only if entry already exists with same value.
        String consulKey = getConsulKey(this.name, k);
        log.trace("Removing if present an entry by KV: '{}' -> '{}' from Consul Async KV store.", consulKey, v.toString());
        assertKeyAndValueAreNotNull(k, v)
                .compose(aVoid -> {
                    Future<V> future = Future.future();
                    get(k, future.completer());
                    return future;
                })
                .compose(value -> {
                    Future<Boolean> future = Future.future();
                    if (v.equals(value)) {
                        consulClient.deleteValue(consulKey, resultDelHandler -> {
                            if (resultDelHandler.succeeded()) {
                                log.trace("Entry KV: '{}' - '{}' has been removed from Consul Async KV store.");
                                future.complete(true);
                            } else {
                                log.error("Error occurred while trying to remove an entry by KV: '{}' -> '{}' from Consul Async KV store. Details: '{}'", consulKey, v.toString(), resultDelHandler.cause().toString());
                                future.fail(resultDelHandler.cause());
                            }
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
        String consulKey = getConsulKey(this.name, k);
        log.trace("Replacing an entry with K: '{}' by new V: '{}'.", consulKey, v.toString());

        assertKeyAndValueAreNotNull(k, v)
                .compose(aVoid -> {
                    Future<V> future = Future.future();
                    get(k, future.completer());
                    return future;

                })
                .compose(value -> {
                    Future<V> future = Future.future();
                    if (Objects.nonNull(value)) {
                        consulClient.putValue(consulKey, v.toString(), resultHandler -> {
                            if (resultHandler.succeeded()) {
                                log.trace("New V: '{}' has replaced the old V: '{}' where K: '{}'.", v.toString(), value, consulKey);
                                future.complete(value);
                            } else {
                                log.error("Error occurred while replacing an entry KnewV: '{}' - '{}'. Details: '{}'", consulKey, v.toString(), resultHandler.cause().toString());
                                future.fail(resultHandler.cause());
                            }
                        });
                    } else {
                        log.trace("Can't replace an entry KnewV: '{}' - '{}' since it doesn't exists.", consulKey, v.toString());
                        future.complete();
                    }
                    return future;
                })
                .setHandler(asyncResultHandler);
    }

    @Override
    public void replaceIfPresent(K k, V oldValue, V newValue, Handler<AsyncResult<Boolean>> resultHandler) {
        // replaces the entry only if it is currently mapped to a specific value.
        String consulKey = getConsulKey(this.name, k);
        assertKeyIsNotNull(k)
                .compose(aVoid -> assertValueIsNotNull(oldValue))
                .compose(aVoid -> assertValueIsNotNull(newValue))
                .compose(aVoid -> {
                    Future<V> future = Future.future();
                    get(k, future.completer());
                    return future;
                })
                .compose(value -> {
                    Future<Boolean> future = Future.future();
                    if (Objects.nonNull(value)) {
                        if (value.equals(oldValue)) {
                            put(k, newValue, resultPutHandler -> {
                                if (resultPutHandler.succeeded()) {
                                    log.trace("Old V: '{}' has been replaced by new V: '{}' where K: '{}'", oldValue.toString(), newValue.toString(), consulKey);
                                    future.complete(true);
                                } else {
                                    log.trace("Can't replace old V: '{}' by new V: '{}' where K: '{}' due to: '{}'", oldValue.toString(), newValue.toString(), consulKey, resultPutHandler.cause().toString());
                                    future.fail(resultPutHandler.cause());
                                }
                            });
                        } else {
                            log.trace("An entry with K: '{}' doesn't map to old V: '{}' so it wo'nt get replaced.", consulKey, oldValue.toString());
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
        log.trace("Clearing all entries in the map: '{}'", this.name);
        Future<Void> future = Future.future();

        consulClient.deleteValues(this.name, resultDelAllHandler -> {
            if (resultDelAllHandler.succeeded()) {
                log.trace("Map: '{}' has been cleared.", this.name);
                future.complete();

            } else {
                log.error("Error occurred while trying to clearing all entries in the map: '{}' due to: '{}'", this.name, resultDelAllHandler.cause().toString());
                future.fail(resultDelAllHandler.cause());
            }
        });
        future.setHandler(resultHandler);
    }

    @Override
    public void size(Handler<AsyncResult<Integer>> resultHandler) {
        log.trace("Calculating the size of: {}", this.name);
        Future<Integer> future = Future.future();

        consulClient.getKeys(this.name, resultSizeHandler -> {
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
        log.trace("Fetching all keys from: {}", this.name);
        Future<Set<K>> future = Future.future();

        consulClient.getKeys(this.name, resultHandler -> {
            if (resultHandler.succeeded()) {
                log.trace("Ks: '{}' of: '{}'", resultHandler.result(), this.name);
                // FIXME
                future.complete(new HashSet<>((List<K>) resultHandler.result()));
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

        consulClient.getValues(this.name, resultHandler -> {
            if (resultHandler.succeeded()) {
                List<String> values = resultHandler.result().getList().stream().map(KeyValue::getValue).collect(Collectors.toList());
                log.trace("Vs: '{}' of: '{}'", values, this.name);
                future.complete((List<V>) values);
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

        consulClient.getValues(this.name, resultHandler -> {
            if (resultHandler.succeeded()) {
                Map<String, String> asyncMap = resultHandler.result().getList().stream().collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));
                log.trace("Listing all entries within async KV store: '{}' is: '{}'", this.name, Json.encodePrettily(asyncMap));
                // FIXME : this is wrong.
                future.complete((Map<K, V>) asyncMap);
            } else {
                log.error("Failed to fetch all entries of async map '{}' due to : '{}'", this.name, resultHandler.cause().toString());
                future.fail(resultHandler.cause());
            }
        });

        future.setHandler(asyncResultHandler);
    }

    // helper method used to print out periodically the async consul map.
    private void printOutAsyncMap() {
        vertx.setPeriodic(5000, event -> entries(Future.future()));
    }
}
