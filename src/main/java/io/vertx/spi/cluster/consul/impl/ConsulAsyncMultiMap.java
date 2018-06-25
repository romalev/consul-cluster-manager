package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ChoosableIterable;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.consul.ConsulClient;

import java.util.function.Predicate;

public class ConsulAsyncMultiMap<K, V> implements AsyncMultiMap<K, V> {

    private final static Logger log = LoggerFactory.getLogger(ConsulAsyncMultiMap.class);

    private final ConsulClient consulClient;
    private final Vertx rxVertx;

    public ConsulAsyncMultiMap(ConsulClient consulClient, Vertx rxVertx) {
        this.consulClient = consulClient;
        this.rxVertx = rxVertx;
    }

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
