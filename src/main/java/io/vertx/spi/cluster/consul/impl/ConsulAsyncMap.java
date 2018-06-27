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
import io.vertx.ext.consul.KeyValue;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Distributed async map implementation based on consul key-value store.
 * <p>
 * TODO: 1) most of logging has to be removed when consul cluster manager is more or less stable.
 * TODO: 2) everything has to be documented in javadocs.
 *
 * @author Roman Levytskyi
 */
public class ConsulAsyncMap<K, V> extends ConsulAsyncAbstractMap<K, V> implements AsyncMap<K, V> {

    private static final Logger log = LoggerFactory.getLogger(ConsulAsyncMap.class);

    private final String name;
    private final Vertx vertx;
    private final ConsulClient consulClient;

    public ConsulAsyncMap(String name, Vertx vertx, ConsulClient consulClient) {
        this.name = name;
        this.vertx = vertx;
        this.consulClient = consulClient;
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
        //    * Put the entry only if there is no entry with the key already present. If key already present then the existing
        //   * value will be returned to the handler, otherwise null.

        assertKeyAndValueAreNotNull(k, v)
                .compose(aVoid -> {
                    Future<V> future = Future.future();
                    get(k, future);
                    return future;
                })
                .compose(vV -> {
                    Future<V> future = Future.future();
                    // TODO : implement this.
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

    }

    @Override
    public void removeIfPresent(K k, V v, Handler<AsyncResult<Boolean>> resultHandler) {

    }

    @Override
    public void replace(K k, V v, Handler<AsyncResult<V>> asyncResultHandler) {

    }

    @Override
    public void replaceIfPresent(K k, V oldValue, V newValue, Handler<AsyncResult<Boolean>> resultHandler) {

    }

    @Override
    public void clear(Handler<AsyncResult<Void>> resultHandler) {

    }

    @Override
    public void size(Handler<AsyncResult<Integer>> resultHandler) {

    }

    @Override
    public void keys(Handler<AsyncResult<Set<K>>> asyncResultHandler) {

    }

    @Override
    public void values(Handler<AsyncResult<List<V>>> asyncResultHandler) {

    }

    @Override
    public void entries(Handler<AsyncResult<Map<K, V>>> asyncResultHandler) {

    }

    // helper method used to print out periodically the async consul map.
    private void printOutAsyncMap() {
        vertx.setPeriodic(5000, event -> {
            consulClient.getValues(this.name, resultHandler -> {
                if (resultHandler.succeeded()) {
                    Map<String, String> asyncMap = resultHandler.result().getList().stream().collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));
                    log.trace("Consul Async KV store: '{}' is: '{}'", this.name, Json.encodePrettily(asyncMap));
                } else {
                    log.error("Failed to print out async map '{}'. Details: '{}'", this.name, resultHandler.cause().toString());
                }
            });
        });
    }
}
