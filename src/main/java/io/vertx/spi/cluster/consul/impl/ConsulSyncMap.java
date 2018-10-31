package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.consul.ConsulClient;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static io.vertx.core.Future.succeededFuture;

/**
 * Distributed sync map implementation based on consul key-value store.
 * It is ONLY used by {@link io.vertx.core.impl.HAManager} - essentially it holds HA INFO about cluster node's vertices.
 * Sync map's entries mode is PERSISTENT (not EPHEMERAL) in order for HA to work correctly.
 *
 * @author Roman Levytskyi
 */
public final class ConsulSyncMap<K, V> extends ConsulMap<K, V> implements Map<K, V> {

  private long timeout = 10000;

  public ConsulSyncMap(String name, String nodeId, Vertx vx, ConsulClient cC) {
    super(name, nodeId, vx, cC);
  }

  @Override
  public int size() {
    return toSync(consulKeys().compose(list -> succeededFuture(list.size())), timeout);
  }

  @Override
  public boolean isEmpty() {
    return toSync(consulKeys().compose(list -> succeededFuture(list.isEmpty())), timeout);
  }

  @Override
  public boolean containsKey(Object key) {
    return toSync(entries().compose(kvMap -> succeededFuture(kvMap.keySet().contains(key))), timeout);
  }

  @Override
  public boolean containsValue(Object value) {
    return toSync(entries().compose(kvMap -> succeededFuture(kvMap.values().contains(value))), timeout);
  }

  @Override
  public V get(Object key) {
    return toSync(getValue((K) key), timeout);
  }

  @Override
  public V put(K key, V value) {
    return toSync(putValue(key, value).compose(aBoolean -> {
      if (aBoolean) return succeededFuture(value);
      else return Future.failedFuture("[" + nodeId + "]" + " failed to put KV: " + key + " -> " + value);
    }), timeout);
  }

  @Override
  public V remove(Object key) {
    return toSync(getValue((K) key).compose(v -> {
      if (v == null) return succeededFuture();
      else return delete((K) key).compose(aBoolean -> {
        if (aBoolean) {
          return succeededFuture(v);
        } else return Future.failedFuture("[" + nodeId + "]" + " failed to remove an entry by K: " + key);
      });
    }), timeout);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    m.forEach(this::put);
  }

  @Override
  public void clear() {
    toSync(deleteAll(), timeout);
  }

  public void clear(Handler<AsyncResult<Void>> handler) {
    deleteAll().setHandler(handler);
  }

  @Override
  public Set<K> keySet() {
    return toSync(entries().compose(kvMap -> succeededFuture(kvMap.keySet())), timeout);
  }

  @Override
  public Collection<V> values() {
    return toSync(entries().compose(kvMap -> succeededFuture(kvMap.values())), timeout);
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return toSync(entries().compose(kvMap -> succeededFuture(kvMap.entrySet())), timeout);
  }
}
