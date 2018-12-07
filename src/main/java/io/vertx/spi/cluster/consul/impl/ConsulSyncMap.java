package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static io.vertx.core.Future.succeededFuture;

/**
 * Distributed sync map implementation based on consul key-value store.
 * It is ONLY used by {@link io.vertx.core.impl.HAManager} - essentially it holds HA INFO about cluster node's vertices.
 * Sync map's entries mode is PERSISTENT (not EPHEMERAL) in order for HA to work correctly.
 *
 * @author <a href="mailto:roman.levytskyi.oss@gmail.com">Roman Levytskyi</a>
 */
public final class ConsulSyncMap<K, V> extends ConsulMap<K, V> implements Map<K, V> {

  private long timeout = 30_000;

  public ConsulSyncMap(String name, ConsulMapContext cmContext) {
    super(name, cmContext);
  }

  @Override
  public int size() {
    return completeAndGet(plainKeys().compose(list -> succeededFuture(list.size())), timeout);
  }

  @Override
  public boolean isEmpty() {
    return completeAndGet(plainKeys().compose(list -> succeededFuture(list.isEmpty())), timeout);
  }

  @Override
  public boolean containsKey(Object key) {
    return completeAndGet(entries().compose(kvMap -> succeededFuture(kvMap.keySet().contains(key))), timeout);
  }

  @Override
  public boolean containsValue(Object value) {
    return completeAndGet(entries().compose(kvMap -> succeededFuture(kvMap.values().contains(value))), timeout);
  }

  @Override
  public V get(Object key) {
    return completeAndGet(getValue((K) key), timeout);
  }

  @Override
  public V put(K key, V value) {
    return completeAndGet(putValue(key, value).compose(aBoolean -> {
      if (aBoolean) return succeededFuture(value);
      else return Future.failedFuture("[" + mapContext.getNodeId() + "]" + " failed to put KV: " + key + " -> " + value);
    }), timeout);
  }

  @Override
  public V remove(Object key) {
    return completeAndGet(getValue((K) key).compose(v -> {
      if (v == null) return succeededFuture();
      else return deleteValue((K) key).compose(aBoolean -> {
        if (aBoolean) {
          return succeededFuture(v);
        } else return Future.failedFuture("[" + mapContext.getNodeId() + "]" + " failed to remove an entry by K: " + key);
      });
    }), timeout);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    m.forEach(this::put);
  }

  @Override
  public void clear() {
    completeAndGet(deleteAll(), timeout);
  }

  // async version of clear - can be executed directly on event loop.
  public void clear(Handler<AsyncResult<Void>> handler) {
    deleteAll().setHandler(handler);
  }

  @Override
  public Set<K> keySet() {
    return completeAndGet(entries().compose(kvMap -> succeededFuture(kvMap.keySet())), timeout);
  }

  @Override
  public Collection<V> values() {
    return completeAndGet(entries().compose(kvMap -> succeededFuture(kvMap.values())), timeout);
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return completeAndGet(entries().compose(kvMap -> succeededFuture(kvMap.entrySet())), timeout);
  }
}
