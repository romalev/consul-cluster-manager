package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Future;

/**
 * @author Roman Levytskyi
 */
abstract class ConsulAsyncAbstractMap<K, V> {

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
}
