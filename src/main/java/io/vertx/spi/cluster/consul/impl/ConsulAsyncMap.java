package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.*;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.KeyValueOptions;

import java.util.*;

import static io.vertx.spi.cluster.consul.impl.ConversationUtils.asFutureConsulEntry;

/**
 * Distributed async map implementation based on consul key-value store.
 * <p>
 * Note: given map is used in vertx shared data - it is used by vertx nodes to share the data, entries of this map are always PERSISTENT not EPHEMERAL.
 *
 * @author Roman Levytskyi
 */
public class ConsulAsyncMap<K, V> extends ConsulMap<K, V> implements AsyncMap<K, V> {

  private static final Logger log = LoggerFactory.getLogger(ConsulAsyncMap.class);

  private final Map<K, V> cache;

  public ConsulAsyncMap(String name, String nodeId, Vertx vertx, ConsulClient cC, CacheManager cM) {
    super(name, nodeId, vertx, cC);
    cache = cM.createAndGetCacheMap(name);
    // TODO : REMOVE IT.
    printOutAsyncMap();
  }

  @Override
  public void get(K k, Handler<AsyncResult<V>> asyncResultHandler) {
    assertKeyIsNotNull(k)
      .compose(aVoid ->
        cache.containsKey(k) ? Future.succeededFuture(cache.get(k)) : getValue(k)
          .compose(value -> {
            // immediately update the cache by new entry.
            if (value != null) cache.put(k, value);
            return Future.succeededFuture(value);
          })).setHandler(asyncResultHandler);
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
      else removeConsulValue(keyPath(k))
        .compose(removeSucceeded -> removeSucceeded ? cacheableRemove(k, v) : Future.failedFuture("Key + " + k + " wasn't removed."))
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
        return removeConsulValue(keyPath(k))
          .compose(removeSucceeded -> removeSucceeded ? cacheableRemove(k) : Future.failedFuture("Key + " + k + " wasn't removed."));
      else return Future.succeededFuture(false);
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
    delete().compose(aVoid -> {
      cache.clear();
      return Future.<Void>succeededFuture();
    }).setHandler(resultHandler);
  }

  @Override
  public void size(Handler<AsyncResult<Integer>> resultHandler) {
    consulKeys().compose(list -> Future.succeededFuture(list.size())).setHandler(resultHandler);
  }

  @Override
  public void keys(Handler<AsyncResult<Set<K>>> asyncResultHandler) {
    entries().compose(kvMap -> Future.succeededFuture(kvMap.keySet())).setHandler(asyncResultHandler);
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
  private Future<Map<K, V>> entries() {
    return consulEntries()
      .compose(kvEntries -> {
        List<Future> futureList = new ArrayList<>();
        kvEntries.getList().forEach(kv -> futureList.add(asFutureConsulEntry(kv.getValue())));
        return CompositeFuture.all(futureList).map(compositeFuture -> {
          Map<K, V> map = new HashMap<>();
          for (int i = 0; i < compositeFuture.size(); i++) {
            ConsulEntry<K, V> consulEntry = compositeFuture.resultAt(i);
            map.put(consulEntry.getKey(), consulEntry.getValue());
          }
          return map;
        });
      });
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
    vertx.setPeriodic(15000, event -> entries(ents -> log.trace("Entries of: " + name + ": " + ents.result())));
  }
}
