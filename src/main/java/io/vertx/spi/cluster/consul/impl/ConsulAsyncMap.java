package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.shareddata.AsyncMap;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConsulAsyncMap<K, V> implements AsyncMap<K, V> {


    @Override
    public void get(K k, Handler<AsyncResult<V>> asyncResultHandler) {

    }

    @Override
    public void put(K k, V v, Handler<AsyncResult<Void>> completionHandler) {

    }

    @Override
    public void put(K k, V v, long ttl, Handler<AsyncResult<Void>> completionHandler) {

    }

    @Override
    public void putIfAbsent(K k, V v, Handler<AsyncResult<V>> completionHandler) {

    }

    @Override
    public void putIfAbsent(K k, V v, long ttl, Handler<AsyncResult<V>> completionHandler) {

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
}
