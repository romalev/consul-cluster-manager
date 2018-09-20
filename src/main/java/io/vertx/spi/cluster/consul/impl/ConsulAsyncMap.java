package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.KeyValueOptions;
import io.vertx.spi.cluster.consul.impl.cache.CacheManager;

import java.util.*;

/**
 * Distributed async map implementation based on consul key-value store.
 * <p>
 * Note: given map is used in vertx shared data - it is used by vertx nodes to share the data, entries of this map are always PERSISTENT not EPHEMERAL.
 *
 * @author Roman Levytskyi
 */
public class ConsulAsyncMap<K, V> extends ConsulMap<K, V> implements AsyncMap<K, V> {

    private static final Logger log = LoggerFactory.getLogger(ConsulAsyncMap.class);

    private final Vertx vertx;
    private final Map<K, V> cache;

    public ConsulAsyncMap(String name, Vertx vertx, ConsulClient cC, CacheManager cM) {
        super(name, cC);
        this.vertx = vertx;
        cache = cM.createAndGetCacheMap(name);
        // TODO : REMOVE IT.
        printOutAsyncMap();
    }

    @Override
    public void get(K k, Handler<AsyncResult<V>> asyncResultHandler) {
        V v = cache.get(k);
        if (v == null) {
            // fallback to original consul store.
            getValue(k)
                    .compose(value -> {
                        // immediately update the cache by new entry.
                        if (value != null) cache.put(k, value);
                        return Future.succeededFuture(value);
                    })
                    .setHandler(asyncResultHandler);
        } else {
            Future.succeededFuture(v).setHandler(asyncResultHandler);
        }
    }

    @Override
    public void put(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
        putValue(k, v)
                .compose(putSucceeded -> putSucceeded ? cacheablePut(k, v) : Future.failedFuture(k.toString() + "wasn't put to: " + name))
                .setHandler(completionHandler);
    }

    @Override
    public void put(K k, V v, long ttl, Handler<AsyncResult<Void>> completionHandler) {
        assertKeyAndValueAreNotNull(k, v)
                .compose(aVoid -> getTtlSessionId(ttl, k))
                .compose(id -> putValue(k, v, new KeyValueOptions().setAcquireSession(id)))
                .compose(putSucceeded -> putSucceeded ? cacheablePut(k, v) : Future.<Void>failedFuture(k.toString() + "wasn't put to " + name))
                .setHandler(completionHandler);
    }

    @Override
    public void putIfAbsent(K k, V v, Handler<AsyncResult<V>> completionHandler) {
        log.trace("'{}' - putting if absent KV: '{}' -> '{}' to CKV.", name, k, v);
        putIfAbsent(k, v, Optional.empty()).setHandler(completionHandler);
    }

    @Override
    public void putIfAbsent(K k, V v, long ttl, Handler<AsyncResult<V>> completionHandler) {
        log.trace("'{}' - putting if absent KV: '{}' -> '{}' to CKV with ttl", name, k, v, ttl);
        assertKeyAndValueAreNotNull(k, v)
                .compose(aVoid -> getTtlSessionId(ttl, k))
                .compose(sessionId -> putIfAbsent(k, v, Optional.of(sessionId)))
                .setHandler(completionHandler);
    }

    @Override
    public void remove(K k, Handler<AsyncResult<V>> asyncResultHandler) {
        log.trace("'{}' - trying to remove an entry by K: '{}' from CKV.", name, k);
        assertKeyIsNotNull(k).compose(aVoid -> {
            Future<V> future = Future.future();
            get(k, future.completer());
            return future;
        }).compose(v -> {
            Future<V> future = Future.future();
            if (v == null) future.complete();
            else removeConsulValue(keyPath(k))
                    .compose(removeSucceeded -> removeSucceeded ? cacheableRemove(k, v) : Future.failedFuture("Key + " + k + " wasn't removed."))
                    .setHandler(future.completer());
            return future;

        }).setHandler(asyncResultHandler);
    }

    @Override
    public void removeIfPresent(K k, V v, Handler<AsyncResult<Boolean>> resultHandler) {
        // removes a value from the map, only if entry already exists with same value.
        log.trace("'{}' - removing if present an entry by KV: '{}' -> '{}' from CKV.", name, k, v);
        assertKeyAndValueAreNotNull(k, v).compose(aVoid -> {
            Future<V> future = Future.future();
            get(k, future.completer());
            return future;
        }).compose(value -> {
            if (v.equals(value))
                return removeConsulValue(keyPath(k))
                        .compose(removeSucceeded -> removeSucceeded ? cacheableRemove(k) : Future.failedFuture("Key + " + k + " wasn't removed."));
            else return Future.succeededFuture(false);
        }).setHandler(resultHandler);
    }

    @Override
    public void replace(K k, V v, Handler<AsyncResult<V>> asyncResultHandler) {
        // replaces the entry only if it is currently mapped to some value.
        log.trace("'{}' - replacing an entry with K: '{}' by new V: '{}'.", name, k, v);
        assertKeyAndValueAreNotNull(k, v).compose(aVoid -> {
            Future<V> future = Future.future();
            get(k, future.completer());
            return future;
        }).compose(value -> {
            Future<V> future = Future.future();
            if (value == null) {
                future.complete();
            } else {
                put(k, v, event -> {
                    if (event.succeeded()) future.complete(value);
                    else future.fail(event.cause());
                });
            }
            return future;
        }).setHandler(asyncResultHandler);
    }

    @Override
    public void replaceIfPresent(K k, V oldValue, V newValue, Handler<AsyncResult<Boolean>> resultHandler) {
        // replaces the entry only if it is currently mapped to a specific value.
        assertKeyAndValueAreNotNull(k, oldValue)
                .compose(aVoid -> assertValueIsNotNull(newValue))
                .compose(aVoid -> {
                    Future<V> future = Future.future();
                    get(k, future.completer());
                    return future;
                })
                .compose(value -> {
                    Future<Boolean> future = Future.future();
                    if (value != null) {
                        if (value.equals(oldValue)) {
                            put(k, newValue, resultPutHandler -> {
                                if (resultPutHandler.succeeded()) {
                                    log.trace("Old V: '{}' has been replaced by new V: '{}' where K: '{}'", oldValue, newValue, k);
                                    future.complete(true);
                                } else {
                                    log.trace("Can't replace old V: '{}' by new V: '{}' where K: '{}' due to: '{}'", oldValue, newValue, k, resultPutHandler.cause().toString());
                                    future.fail(resultPutHandler.cause());
                                }
                            });
                        } else {
                            log.trace("An entry with K: '{}' doesn't map to old V: '{}' so it won't get replaced.", k, oldValue);
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
        cache.clear();
        clearUp().setHandler(resultHandler);
    }

    @Override
    public void size(Handler<AsyncResult<Integer>> resultHandler) {
        Future.succeededFuture(cache.size()).setHandler(resultHandler);

    }

    @Override
    public void keys(Handler<AsyncResult<Set<K>>> asyncResultHandler) {
        Future.succeededFuture(cache.keySet()).setHandler(asyncResultHandler);
    }

    @Override
    public void values(Handler<AsyncResult<List<V>>> asyncResultHandler) {
        List<V> values = new ArrayList<>(cache.values());
        Future.succeededFuture(values).setHandler(asyncResultHandler);
    }

    @Override
    public void entries(Handler<AsyncResult<Map<K, V>>> asyncResultHandler) {
        Future.succeededFuture(cache).setHandler(asyncResultHandler);
    }

    /**
     * Puts the entry only if there is no entry with the key already present. If key already present then the existing
     * value will be returned to the handler, otherwise null.
     *
     * @param k         - holds the entry's key.
     * @param v         - holds the entry's value.
     * @param sessionId - holds the ttl session id.
     * @return future existing value if k is already present, otherwise future null.
     */
    private Future<V> putIfAbsent(K k, V v, Optional<String> sessionId) {
        Future<V> future = Future.future();
        KeyValueOptions casOpts = new KeyValueOptions();
        sessionId.ifPresent(casOpts::setAcquireSession);
        sessionId.orElseGet(() -> {
            // set the Check-And-Set index. If the index is {@code 0}, Consul will only put the key if it does not already exist.
            casOpts.setCasIndex(0);
            return null;
        });
        return putValue(k, v, casOpts).compose(putSucceeded -> {
            if (putSucceeded) {
                cache.put(k, v);
                future.complete();
            } else get(k, future.completer()); // key already present
            return future;
        });
    }

    /**
     * @param k - holds the key.
     * @param v - holds the value.
     * @return succeeded future with updating internal cache appropriately.
     */
    private Future<Void> cacheablePut(K k, V v) {
        cache.put(k, v);
        return Future.succeededFuture();
    }

    /**
     * @param k - holds the key.
     * @return succeeded future with updating internal cache appropriately.
     */
    private Future<Boolean> cacheableRemove(K k) {
        cache.remove(k);
        return Future.succeededFuture(true);
    }

    /**
     * @param k - holds the key.
     * @param v - holds the value.
     * @return succeeded future with updating internal cache appropriately.
     */
    private Future<V> cacheableRemove(K k, V v) {
        cache.remove(k);
        return Future.succeededFuture(v);
    }

    // helper method used to print out periodically the async consul map.
    // TODO: remove it.
    private void printOutAsyncMap() {
        vertx.setPeriodic(15000, event -> entries(Future.future()));
    }
}
