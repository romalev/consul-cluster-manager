package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.KeyValueOptions;

import java.util.*;

import static io.vertx.spi.cluster.consul.impl.Utils.decode;
import static io.vertx.spi.cluster.consul.impl.Utils.encodeInFuture;

/**
 * Provides specific functionality for async clustering maps.
 *
 * @author Roman Levytskyi
 */
abstract class ConsulMap<K, V> {

    private static final Logger log = LoggerFactory.getLogger(ConsulMap.class);

    final ConsulClient consulClient;
    final String name;
    final String sessionId;
    final KeyValueOptions defaultKvOptions;

    private final EnumSet<ClusterManagerMaps> clusterManagerMaps = EnumSet.of(ClusterManagerMaps.VERTX_HA_INFO, ClusterManagerMaps.VERTX_SUBS);

    ConsulMap(ConsulClient consulClient, String name, String sessionId) {
        this.consulClient = consulClient;
        this.name = name;
        this.sessionId = sessionId;
        this.defaultKvOptions = clusterManagerMaps.contains(ClusterManagerMaps.fromString(name)) ? new KeyValueOptions().setAcquireSession(sessionId) : null;
    }

    Future<Void> putValue(K k, V v) {
        log.trace("'{}' - trying to put KV: '{}'->'{}' CKV.", name, k, v);
        return assertKeyAndValueAreNotNull(k, v)
                .compose(aVoid -> encodeInFuture(v))
                .compose(value -> putValueWithOptions(getConsulKey(name, k), value, defaultKvOptions));
    }

    Future<Void> putValue(K k, V v, KeyValueOptions keyValueOptions) {
        log.trace("'{}' - trying to put KV: '{}'->'{}' CKV.", name, k, v);
        return assertKeyAndValueAreNotNull(k, v)
                .compose(aVoid -> encodeInFuture(v))
                .compose(value -> putValueWithOptions(getConsulKey(name, k), value, keyValueOptions));
    }

    Future<V> removeValue(K k) {
        log.trace("'{}' - trying to remove an entry by K: '{}' from CKV.", name, k);
        return assertKeyIsNotNull(k)
                .compose(aVoid -> getValue(k))
                .compose(v -> {
                    Future<V> future = Future.future();
                    consulClient.deleteValue(getConsulKey(name, k), resultHandler -> {
                        if (resultHandler.succeeded()) {
                            log.trace("'{}' - K: '{}' has been removed from CKV.", name, k.toString());
                            future.complete(v);
                        } else {
                            log.trace("'{}' - Can't delete K: '{}' from CKV due to: '{}'.", name, k.toString(), resultHandler.cause().toString());
                            future.fail(resultHandler.cause());
                        }
                    });
                    return future;
                });
    }

    Future<V> getValue(K k) {
        log.trace("'{}' - getting an entry by K: '{}' from CKV.", name, k);
        return assertKeyIsNotNull(k)
                .compose(aVoid -> {
                    Future<V> future = Future.future();
                    final String consulKey = getConsulKey(name, k);
                    consulClient.getValue(consulKey, resultHandler -> {
                        if (resultHandler.succeeded()) {
                            if (Objects.nonNull(resultHandler.result()) && Objects.nonNull(resultHandler.result().getValue())) {
                                log.trace("'{}' - got an entry '{}' - '{}'", name, k.toString(), resultHandler.result().getValue());
                                try {
                                    future.complete(decode(resultHandler.result().getValue()));
                                } catch (Exception e) {
                                    future.fail(e.getCause());
                                }
                            } else {
                                log.trace("'{}' - nothing is found by: '{}'", name, k.toString());
                                future.complete();
                            }
                        } else {
                            log.error("Failed to get an entry by K: '{}' from Consul Async KV store. Details: '{}'", k.toString(), resultHandler.cause().toString());
                            future.fail(resultHandler.cause());
                        }
                    });
                    return future;
                });
    }

    Future<Void> clearUp() {
        Future<Void> future = Future.future();
        log.trace("{} - clearing this up.", name);
        consulClient.deleteValues(name, result -> {
            if (result.succeeded()) {
                log.trace("'{}' - has been cleared.");
                future.complete();
            } else {
                log.trace("Can't clear: '{}' due to: '{}'", result.cause().toString());
                future.fail(result.cause());
            }
        });
        return future;
    }

    Future<Set<K>> keys() {
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
        return future;
    }

    /**
     * Verifies whether value is not null.
     */
    Future<Void> assertValueIsNotNull(Object value) {
        boolean result = value == null;
        if (result) return io.vertx.core.Future.failedFuture("Value can not be null.");
        else return Future.succeededFuture();
    }

    /**
     * Verifies whether key & value are not null.
     */
    Future<Void> assertKeyAndValueAreNotNull(Object key, Object value) {
        return assertKeyIsNotNull(key).compose(aVoid -> assertValueIsNotNull(value));
    }

    /**
     * Verifies whether key is not null.
     */
    Future<Void> assertKeyIsNotNull(Object key) {
        boolean result = key == null;
        if (result) return io.vertx.core.Future.failedFuture("Key can not be null.");
        else return io.vertx.core.Future.succeededFuture();
    }

    String getConsulKey(String name, K k) {
        return name + "/" + k.toString();
    }

    private Future<Void> putValueWithOptions(String key, String value, KeyValueOptions keyValueOptions) {
        Future<Void> future = Future.future();
        consulClient.putValueWithOptions(key, value, keyValueOptions, resultHandler -> {
            if (resultHandler.succeeded()) {
                log.trace("'{}'- KV: '{}'->'{}' has been put to CKV.", name, key, value);
                future.complete();
            } else {
                log.error("'{}' - Can't put KV: '{}'->'{}' to CKV due to: '{}'", name, key, value, future.cause().toString());
                future.fail(resultHandler.cause());
            }
        });
        return future;
    }
}
