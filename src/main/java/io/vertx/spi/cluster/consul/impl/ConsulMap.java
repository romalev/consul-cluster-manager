package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueList;
import io.vertx.ext.consul.KeyValueOptions;

import java.util.Collections;
import java.util.List;

import static io.vertx.spi.cluster.consul.impl.ClusterSerializationUtils.decodeF;
import static io.vertx.spi.cluster.consul.impl.ClusterSerializationUtils.encodeF;

/**
 * Abstract map functionality for clustering maps.
 *
 * @author Roman Levytskyi
 */
abstract class ConsulMap<K, V> {

    private static final Logger log = LoggerFactory.getLogger(ConsulMap.class);

    final String name;
    final ConsulClient consulClient;

    ConsulMap(String name, ConsulClient consulClient) {
        this.name = name;
        this.consulClient = consulClient;
    }

    /**
     * Puts an entry to Consul KV store.
     *
     * @param k - holds the key of an entry.
     * @param v - holds the value of an entry.
     * @return succeededFuture indicating that an entry has been put, failedFuture - otherwise.
     */
    Future<Boolean> putValue(K k, V v) {
        return putValue(k, v, null);
    }

    /**
     * Puts an entry to Consul KV store by taking into account additional options : these options are mainly used to make an entry ephemeral or
     * to place TTL on an entry.
     *
     * @param k               - holds the key of an entry.
     * @param v               - holds the value of an entry.
     * @param keyValueOptions - holds kv options (note: null is allowed)
     * @return succeededFuture indicating that an entry has been put, failedFuture - otherwise.
     */
    Future<Boolean> putValue(K k, V v, KeyValueOptions keyValueOptions) {
        log.trace("'{}' - trying to put KV: '{}'->'{}' CKV.", name, k, v);
        return assertKeyAndValueAreNotNull(k, v)
                .compose(aVoid -> encodeF(v))
                .compose(value -> putConsulValue(getConsulKey(name, k), value, keyValueOptions));
    }

    /**
     * Puts an entry to Consul KV store. Does the actual job.
     *
     * @param key             - holds the consul key of an entry.
     * @param value           - holds the consul value (should be marshaled) of an entry.
     * @param keyValueOptions - holds kv options (note: null is allowed)
     * @return booleanFuture indication the put result (true - an entry has been put, false - otherwise), failedFuture - otherwise.
     */
    Future<Boolean> putConsulValue(String key, String value, KeyValueOptions keyValueOptions) {
        Future<Boolean> future = Future.future();
        consulClient.putValueWithOptions(key, value, keyValueOptions, resultHandler -> {
            if (resultHandler.succeeded()) {
                log.trace("'{}'- KV: '{}'->'{}' has been put to CKV.", name, key, value);
                future.complete(resultHandler.result());
            } else {
                log.error("'{}' - Can't put KV: '{}'->'{}' to CKV due to: '{}'", name, key, value, future.cause().toString());
                future.fail(resultHandler.cause());
            }
        });
        return future;
    }

    /**
     * Gets the value by key.
     *
     * @param k - holds the key.
     * @return either empty future if key doesn't exist in KV store, future containing the value if key exists, failedFuture - otherwise.
     */
    Future<V> getValue(K k) {
        log.trace("'{}' - getting an entry by K: '{}' from CKV.", name, k);
        return assertKeyIsNotNull(k)
                .compose(aVoid -> getConsulKeyValue(getConsulKey(name, k)))
                .compose(consulValue -> decodeF(consulValue.getValue()));
    }

    /**
     * Gets the value by consul key.
     *
     * @param consulKey - holds the consul key.
     * @return either empty future if key doesn't exist in KV store, future containing the value if key exists, failedFuture - otherwise.
     */
    Future<KeyValue> getConsulKeyValue(String consulKey) {
        Future<KeyValue> future = Future.future();
        consulClient.getValue(consulKey, resultHandler -> {
            if (resultHandler.succeeded()) {
                // note: resultHandler.result().getValue() is null if nothing was found.
                log.trace("'{}' - got KV: '{}' - '{}'", name, consulKey, resultHandler.result().getValue());
                future.complete(resultHandler.result());
            } else {
                log.error("'{}' - can't get an entry by: '{}'", name, consulKey);
                future.fail(resultHandler.cause());
            }
        });
        return future;
    }

    /**
     * Removes the entry.
     *
     * @param k - holds the key.
     * @return
     */
    Future<V> removeValue(K k) {
        log.trace("'{}' - trying to remove an entry by K: '{}' from CKV.", name, k);
        return getValue(k)
                .compose(v -> {
                    Future<V> future = Future.future();
                    if (v == null) {
                        future.complete();
                    } else {
                        consulClient.deleteValue(getConsulKey(name, k), resultHandler -> {
                            if (resultHandler.succeeded()) {
                                log.trace("'{}' - K: '{}' has been removed.", name, k.toString());
                                future.complete(v);
                            } else {
                                log.trace("'{}' - Can't delete K: '{}' due to: '{}'.", name, k.toString(), resultHandler.cause().toString());
                                future.fail(resultHandler.cause());
                            }
                        });
                    }
                    return future;
                });
    }

    Future<Boolean> removeConsulValue(String key) {
        Future<Boolean> result = Future.future();
        consulClient.deleteValue(key, resultHandler -> {
            if (resultHandler.succeeded()) {
                log.trace("'{}' - K: '{}' has been removed.", name, key);
                result.complete(true);
            } else {
                log.trace("'{}' - Can't delete K: '{}' due to: '{}'.", name, key, resultHandler.cause().toString());
                result.fail(resultHandler.cause());
            }
        });
        return result;
    }


    /**
     * Clears the entire map.
     */
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

    /**
     * @return map's keys
     */
    Future<List<String>> consulKeys() {
        Future<List<String>> futureKeys = Future.future();
        consulClient.getKeys(name, resultHandler -> {
            if (resultHandler.succeeded()) {
                log.trace("Keys of: '{}' are: '{}'", name, resultHandler.result());
                futureKeys.complete(resultHandler.result());
            } else {
                log.error("Error occurred while fetching all the keys from: '{}' due to: '{}'", name, resultHandler.cause().toString());
                futureKeys.fail(resultHandler.cause());
            }
        });
        return futureKeys;
    }

    /**
     * @return map's key value list
     */
    Future<KeyValueList> consulEntries() {
        Future<KeyValueList> keyValueListFuture = Future.future();
        consulClient.getValues(name, resultHandler -> {
            if (resultHandler.succeeded()) keyValueListFuture.complete(resultHandler.result());
            else {
                log.error("Can't get KV List of: '{}' due to: '{}'", name, resultHandler.cause().toString());
                keyValueListFuture.fail(resultHandler.cause());
            }
        });
        return keyValueListFuture;
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

    <T> List<T> getListResult(List<T> list) {
        return list == null ? Collections.emptyList() : list;
    }

    List<KeyValue> getListResult(KeyValueList keyValueList) {
        return keyValueList == null || keyValueList.getList() == null ? Collections.emptyList() : keyValueList.getList();
    }
}
