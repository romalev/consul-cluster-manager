package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.consul.*;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
                .compose(value -> putConsulValue(keyPath(k), value, keyValueOptions));
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
                log.trace("'{}'- put KV: '{}'->'{}' is: '{}'", name, key, value, resultHandler.result());
                future.complete(resultHandler.result());
            } else {
                log.error("'{}' - Can't put KV: '{}'->'{}' to CKV due to: '{}'", name, key, value, resultHandler.cause());
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
                .compose(aVoid -> getConsulKeyValue(keyPath(k)))
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
                log.error("'{}' - can't get an entry by: '{}' due to: '{}'", name, consulKey, resultHandler.cause());
                future.fail(resultHandler.cause());
            }
        });
        return future;
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
     * Creates TTL dedicated consul session. TTL on entries is handled by relaying on consul session itself.
     * We have to register the session first in consul and then bound the session's id with entries we want to put ttl on.
     * <p>
     * Note: session invalidation-time is twice the TTL time -> https://github.com/hashicorp/consul/issues/1172
     * (This is done on purpose. The contract of the TTL is that it will not expire before that value, but could expire after.
     * There are number of reasons for that (complexity during leadership transition), but consul devs add a grace period to account for clock skew and network delays.
     * This is to shield the application from dealing with that.)
     *
     * @param ttl - holds ttl in ms, this value must be between {@code 10s} and {@code 86400s} currently.
     * @return session id.
     */
    Future<String> getTtlSessionId(long ttl, K k) {
        if (ttl < 10000) {
            log.warn("Specified ttl is less than allowed in consul -> min ttl is 10s.");
            ttl = 10000;
        }

        if (ttl > 86400000) {
            log.warn("Specified ttl is more that allowed in consul -> max ttl is 86400s.");
            ttl = 86400000;
        }

        String consulKey = keyPath(k);
        String sessionName = "ttlSession_" + consulKey;
        Future<String> future = Future.future();
        SessionOptions sessionOpts = new SessionOptions()
                .setTtl(TimeUnit.MILLISECONDS.toSeconds(ttl))
                .setBehavior(SessionBehavior.DELETE)
                // Lock delay is a time duration, between 0 and 60 seconds. When a session invalidation takes place,
                // Consul prevents any of the previously held locks from being re-acquired for the lock-delay interval
                .setLockDelay(0)
                .setName(sessionName);

        consulClient.createSessionWithOptions(sessionOpts, idHandler -> {
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

    String keyPath(K k) {
        return name + "/" + k.toString();
    }

    /**
     * Returns NULL - safe key value list - simple wrapper around getting list out of {@link KeyValueList} instance.
     */
    List<KeyValue> nullSafeListResult(KeyValueList keyValueList) {
        return keyValueList == null || keyValueList.getList() == null ? Collections.emptyList() : keyValueList.getList();
    }
}
