package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.shareddata.Counter;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.KeyValueOptions;

/**
 * Consul-based implementation of an asynchronous (distributed) counter that can be used to across the cluster to maintain a consistent count.
 * <p>
 * <b> Given implementation is based on Check-and-Set consul operation.</b>
 * Some notes:
 * <p>
 * The purpose of the Check-and-Set operation is to avoid lost updates when multiple clients are simultaneously trying to update a value
 * of the same key. Check-and-Set operation allows the update to happen only if the value has not been changed since the client last read it.
 * If the current value does not match what the client previously read, the client will receive a conflicting update error message and
 * will have to retry the read-update cycle. The Check-and-Set operation can be used to implement a shared counter, semaphore or a distributed lock
 * - and this is what we need.
 * <p>
 * Good to read: http://alesnosek.com/blog/2017/07/25/check-and-set-operation-and-transactions-in-consul/
 *
 * @author Roman Levytskyi
 */
public class ConsulCounter extends ConsulMap<String, Long> implements Counter {

    // key to access counter.
    private final String consulKey;

    public ConsulCounter(String name, ConsulClient cC) {
        super("__vertx.counters", cC);
        this.consulKey = keyPath(name);
    }

    @Override
    public void get(Handler<AsyncResult<Long>> resultHandler) {
        getConsulKeyValue(consulKey)
                .map(keyValue -> Long.parseLong(keyValue.getValue()))
                .setHandler(resultHandler);
    }

    @Override
    public void incrementAndGet(Handler<AsyncResult<Long>> resultHandler) {
        calculateAndCompareAndSwap(true, 1L, resultHandler);
    }

    @Override
    public void getAndIncrement(Handler<AsyncResult<Long>> resultHandler) {
        calculateAndCompareAndSwap(false, 1L, resultHandler);
    }

    @Override
    public void decrementAndGet(Handler<AsyncResult<Long>> resultHandler) {
        calculateAndCompareAndSwap(true, -1L, resultHandler);
    }

    @Override
    public void addAndGet(long value, Handler<AsyncResult<Long>> resultHandler) {
        calculateAndCompareAndSwap(true, value, resultHandler);
    }

    @Override
    public void getAndAdd(long value, Handler<AsyncResult<Long>> resultHandler) {
        calculateAndCompareAndSwap(false, value, resultHandler);
    }

    @Override
    public void compareAndSet(long expected, long value, Handler<AsyncResult<Boolean>> resultHandler) {
        getConsulKeyValue(consulKey)
                .compose(keyValue -> {
                    Future<Boolean> result = Future.future();
                    final Long preValue = Long.parseLong(keyValue.getValue());
                    if (preValue == expected) {
                        putConsulValue(consulKey, String.valueOf(value), null).setHandler(result.completer());
                    } else {
                        result.complete(false);
                    }
                    return result;
                })
                .setHandler(resultHandler);
    }

    /**
     * Performs calculation operation on the counter.
     */
    private void calculateAndCompareAndSwap(boolean postGet, Long value, Handler<AsyncResult<Long>> resultHandler) {
        getConsulKeyValue(consulKey)
                .compose(keyValue -> {
                    Future<Long> result = Future.future();
                    final Long preValue = Long.parseLong(keyValue.getValue());
                    final Long postValue = Long.parseLong(keyValue.getValue()) + value;
                    putConsulValue(consulKey, String.valueOf(postValue), new KeyValueOptions().setCasIndex(keyValue.getModifyIndex()))
                            .setHandler(putRes -> {
                                if (putRes.succeeded()) {
                                    if (putRes.result()) {
                                        result.complete(postGet ? postValue : preValue);
                                    } else {
                                        // do retry
                                        // TODO: open question : overflow possible ? - define number of allowed retries ???
                                        calculateAndCompareAndSwap(postGet, value, result.completer());
                                    }
                                } else {
                                    result.fail(putRes.cause());
                                }
                            });
                    return result;
                })
                .setHandler(resultHandler);
    }
}
