package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.KeyValueOptions;

import java.util.*;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;

/**
 * Distributed async map implementation based on consul key-value store.
 * <p>
 * Note: given map is used in vertx shared data - it is used by vertx nodes to share the data,
 * entries of this map are always PERSISTENT and NOT EPHEMERAL.
 *
 * @author Roman Levytskyi
 */
public class ConsulAsyncMap<K, V> extends ConsulMap<K, V> implements AsyncMap<K, V> {

  public ConsulAsyncMap(String name, String nodeId, Vertx vertx, ConsulClient cC) {
    super(name, nodeId, vertx, cC);
  }

  @Override
  public void get(K k, Handler<AsyncResult<V>> asyncResultHandler) {
    assertKeyIsNotNull(k)
      .compose(aVoid -> getValue(k))
      .setHandler(asyncResultHandler);
  }

  @Override
  public void put(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
    putValue(k, v)
      .compose(putSucceeded -> putSucceeded ? Future.<Void>succeededFuture() : failedFuture(k.toString() + "wasn't put to: " + name))
      .setHandler(completionHandler);
  }

  @Override
  public void put(K k, V v, long ttl, Handler<AsyncResult<Void>> completionHandler) {
    assertKeyAndValueAreNotNull(k, v)
      .compose(aVoid -> getTtlSessionId(ttl, k))
      .compose(id -> putValue(k, v, new KeyValueOptions().setAcquireSession(id)))
      .compose(putSucceeded -> putSucceeded ? succeededFuture() : Future.<Void>failedFuture(k.toString() + "wasn't put to " + name))
      .setHandler(completionHandler);
  }

  @Override
  public void putIfAbsent(K k, V v, Handler<AsyncResult<V>> completionHandler) {
    putIfAbsent(k, v, Optional.empty()).setHandler(completionHandler);
  }

  @Override
  public void putIfAbsent(K k, V v, long ttl, Handler<AsyncResult<V>> completionHandler) {
    assertKeyAndValueAreNotNull(k, v)
      .compose(aVoid -> getTtlSessionId(ttl, k))
      .compose(sessionId -> putIfAbsent(k, v, Optional.of(sessionId)))
      .setHandler(completionHandler);
  }

  @Override
  public void remove(K k, Handler<AsyncResult<V>> asyncResultHandler) {
    assertKeyIsNotNull(k).compose(aVoid -> {
      Future<V> future = Future.future();
      get(k, future.completer());
      return future;
    }).compose(v -> {
      Future<V> future = Future.future();
      if (v == null) future.complete();
      else deleteConsulValue(keyPath(k))
        .compose(removeSucceeded -> removeSucceeded ? succeededFuture(v) : failedFuture("Key + " + k + " wasn't removed."))
        .setHandler(future.completer());
      return future;

    }).setHandler(asyncResultHandler);
  }

  @Override
  public void removeIfPresent(K k, V v, Handler<AsyncResult<Boolean>> resultHandler) {
    // removes a value from the map, only if entry already exists with same value.
    assertKeyAndValueAreNotNull(k, v).compose(aVoid -> {
      Future<V> future = Future.future();
      get(k, future.completer());
      return future;
    }).compose(value -> {
      if (v.equals(value))
        return deleteConsulValue(keyPath(k))
          .compose(removeSucceeded -> removeSucceeded ? succeededFuture(true) : failedFuture("Key + " + k + " wasn't removed."));
      else return succeededFuture(false);
    }).setHandler(resultHandler);
  }

  @Override
  public void replace(K k, V v, Handler<AsyncResult<V>> asyncResultHandler) {
    // replaces the entry only if it is currently mapped to some value.
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
          if (value.equals(oldValue))
            put(k, newValue, resultPutHandler -> {
              if (resultPutHandler.succeeded())
                future.complete(true); // old V: '{}' has been replaced by new V: '{}' where K: '{}'", oldValue, newValue, k
              else
                future.fail(resultPutHandler.cause()); // failed replace old V: '{}' by new V: '{}' where K: '{}' due to: '{}'", oldValue, newValue, k, resultPutHandler.cause()
            });
          else
            future.complete(false); // "An entry with K: '{}' doesn't map to old V: '{}' so it won't get replaced.", k, oldValue);
        } else future.complete(false); // An entry with K: '{}' doesn't exist,
        return future;
      })
      .setHandler(resultHandler);
  }

  @Override
  public void clear(Handler<AsyncResult<Void>> resultHandler) {
    deleteAll().setHandler(resultHandler);
  }

  @Override
  public void size(Handler<AsyncResult<Integer>> resultHandler) {
    consulKeys().compose(list -> succeededFuture(list.size())).setHandler(resultHandler);
  }

  @Override
  public void keys(Handler<AsyncResult<Set<K>>> asyncResultHandler) {
    entries().compose(kvMap -> succeededFuture(kvMap.keySet())).setHandler(asyncResultHandler);
  }

  @Override
  public void values(Handler<AsyncResult<List<V>>> asyncResultHandler) {
    entries().compose(kvMap -> Future.<List<V>>succeededFuture(new ArrayList<>(kvMap.values())).setHandler(asyncResultHandler));
  }

  @Override
  public void entries(Handler<AsyncResult<Map<K, V>>> asyncResultHandler) {
    entries().setHandler(asyncResultHandler);
  }

  /**
   * Given implementation does NOT take advantage of internal cache :(
   * <p>
   * POSSIBLE IMPROVEMENT: adjust internal cache to be based on MultiKeyMap
   * so then we can query it by actual consul kv store keys (which are not being encoded and are plain strings)
   *
   * @return entries from consul kv store.
   */
  @Override
  Future<Map<K, V>> entries() {
    return super.entries();
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
      if (putSucceeded) future.complete();
      else get(k, future.completer()); // key already present
      return future;
    });
  }
}
