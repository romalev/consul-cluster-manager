package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.consul.KeyValueOptions;

import java.util.*;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static io.vertx.spi.cluster.consul.impl.ConversationUtils.asFutureString;
import static io.vertx.spi.cluster.consul.impl.ConversationUtils.asTtlConsulEntry;

/**
 * Distributed async map implementation based on consul key-value store.
 * <p>
 * Note: given map is used in vertx shared data - it is used by vertx nodes to share the data,
 * entries of this map are always PERSISTENT and NOT EPHEMERAL.
 *
 * @author Roman Levytskyi
 */
public class ConsulAsyncMap<K, V> extends ConsulMap<K, V> implements AsyncMap<K, V> {

  private static final Logger log = LoggerFactory.getLogger(ConsulAsyncMap.class);
  private final TTLMonitor ttlMonitor;

  public ConsulAsyncMap(String name, ConsulMapContext context, ClusterManager clusterManager) {
    super(name, context);
    this.ttlMonitor = new TTLMonitor(mapContext.getVertx(), clusterManager, mapContext.getNodeId());
    startListening();
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
      .compose(id -> putValue(k, v, ttl))
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
      .compose(aVoid -> putIfAbsent(k, v, Optional.of(ttl)))
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
      else deleteValueByKeyPath(keyPath(k))
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
        return deleteValueByKeyPath(keyPath(k))
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
    plainKeys().compose(list -> succeededFuture(list.size())).setHandler(resultHandler);
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

  @Override
  protected void entryUpdated(EntryEvent event) {
    if (event.getEventType() == EntryEvent.EventType.WRITE) {
      log.debug("[" + mapContext.getNodeId() + "] : " + "applying a ttl monitor on entry: " + event.getEntry().getKey());
      ttlMonitor.apply(
        event.getEntry().getKey(),
        () -> deleteValueByKeyPath(event.getEntry().getKey()),
        asTtlConsulEntry(event.getEntry().getValue()));
    }
  }

  @Override
  protected Future<Boolean> putValue(K k, V v, KeyValueOptions keyValueOptions) {
    return super.putValue(k, v, keyValueOptions)
      .compose(result ->
        ttlMonitor.apply(keyPath(k), () -> deleteValue(k), Optional.empty())
          .compose(aVoid -> succeededFuture(result)));
  }

  @Override
  protected Future<Boolean> putValue(K k, V v, long ttl) {
    return super.putValue(k, v, ttl)
      .compose(result ->
        ttlMonitor.apply(keyPath(k), () -> deleteValue(k), Optional.of(ttl))
          .compose(aVoid -> succeededFuture(result)));
  }

  private Future<Boolean> putValue(K k, V v, KeyValueOptions keyValueOptions, Optional<Long> ttl) {
    Long ttlValue = ttl.map(aLong -> ttl.get()).orElse(null);
    return asFutureString(k, v, mapContext.getNodeId(), ttlValue)
      .compose(value -> putPlainValue(keyPath(k), value, keyValueOptions))
      .compose(result ->
        ttlMonitor.apply(keyPath(k), () -> deleteValue(k), ttl)
          .compose(aVoid -> succeededFuture(result)));
  }

  /**
   * Puts the entry only if there is no entry with the key already present. If key already present then the existing
   * value will be returned to the handler, otherwise null.
   *
   * @param k - holds the entry's key.
   * @param v - holds the entry's value.
   * @return future existing value if k is already present, otherwise future null.
   */
  private Future<V> putIfAbsent(K k, V v, Optional<Long> ttl) {
    // set the Check-And-Set index. If the index is {@code 0}, Consul will only put the key if it does not already exist.
    KeyValueOptions casOpts = new KeyValueOptions().setCasIndex(0);
    return putValue(k, v, casOpts, ttl).compose(putSucceeded -> {
      if (putSucceeded) {
        return ttlMonitor.apply(keyPath(k), () -> deleteValue(k), ttl)
          .compose(aVoid -> succeededFuture());
      } else return getValue(k); // key already present
    });
  }

  /**
   * Dedicated to {@link AsyncMap} TTL monitor to handle the ability to place ttl on map's entries.
   * <p>
   * IMPORTANT:
   * TTL can placed on consul entries by relaying on consul sessions (first we have to register ttl session and bind it with an entry) - once
   * session gets expired (session's ttl gets expired) -> all entries given session was bound to get removed automatically from consul kv store.
   * Consul's got following restriction :
   * - Session TTL value must be between 10s and 86400s -> we can't really rely on using consul sessions since it breaks general
   * vert.x cluster management SPI. This also should be taken into account:
   * [Invalidation-time is twice the TTL time](https://github.com/hashicorp/consul/issues/1172) -> actual time when ttl entry gets removed (expired)
   * is doubled to what you will specify as a ttl.
   * <p>
   * For these reasons custom TTL monitoring mechanism was developed.
   *
   * @author Roman Levytskyi.
   */
  private static class TTLMonitor {
    private final static Logger log = LoggerFactory.getLogger(TTLMonitor.class);
    private final Vertx vertx;
    private final ClusterManager clusterManager;
    private final LocalMap<String, Long> timerMap;
    private final String nodeId;

    TTLMonitor(Vertx vertx, ClusterManager clusterManager, String nodeId) {
      this.vertx = vertx;
      this.timerMap = vertx.sharedData().getLocalMap("timerMap");
      this.clusterManager = clusterManager;
      this.nodeId = nodeId;
    }

    Future<Void> apply(String keyPath, Runnable action, Optional<Long> ttl) {
      Future<Void> applied = Future.future();
      if (ttl.isPresent()) {
        log.debug("[" + nodeId + "] : " + "applying ttl monitor on: " + keyPath + " with ttl: " + ttl.get());
        clusterManager.getLockWithTimeout("ttlLockOn/" + keyPath, 50, lockObtainedEvent -> {
          if (lockObtainedEvent.succeeded()) {
            long timerId = setTimer(keyPath, action, ttl.get(), lockObtainedEvent.result());
            timerMap.put(keyPath, timerId);
            applied.complete();
          } else {
            // other node has already obtained a lock.
            applied.complete();
          }
        });
      } else {
        // there's no need to monitor an entry -> no ttl is there.
        // try to remove keyPath from timerMap since
        Long timerId = timerMap.get(keyPath);
        if (Objects.nonNull(timerId)) {
          vertx.cancelTimer(timerId);
          timerMap.remove(keyPath);
          log.debug("[" + nodeId + "] : " + "cancelling ttl monitor on entry: " + keyPath);
        }
        applied.complete();
      }
      return applied;
    }

    private long setTimer(String keyPath, Runnable action, long ttl, Lock lock) {
      return vertx.setTimer(ttl, event -> {
        try {
          action.run();
        } finally {
          log.debug("[" + nodeId + "] : " + "ttl monitor executes deleting of: " + keyPath + " since ttl: " + ttl + " got expired.");
          lock.release();
          timerMap.remove(keyPath);
        }
      });
    }
  }
}
