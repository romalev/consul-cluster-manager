package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ChoosableIterable;

import java.util.function.Predicate;

public class ConsulAsyncMultiMap<K, V> implements AsyncMultiMap<K, V> {

    @Override
    public void add(K k, V v, Handler<AsyncResult<Void>> completionHandler) {

    }

    @Override
    public void get(K k, Handler<AsyncResult<ChoosableIterable<V>>> asyncResultHandler) {

    }

    @Override
    public void remove(K k, V v, Handler<AsyncResult<Boolean>> completionHandler) {

    }

    @Override
    public void removeAllForValue(V v, Handler<AsyncResult<Void>> completionHandler) {

    }

    @Override
    public void removeAllMatching(Predicate<V> p, Handler<AsyncResult<Void>> completionHandler) {

    }
}
