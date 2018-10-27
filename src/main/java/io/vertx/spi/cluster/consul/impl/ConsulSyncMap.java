package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.KeyValueOptions;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Distributed sync map implementation based on consul key-value store.
 * Essentially it holds HA INFO about cluster node's vertices which means map's entries are (AND ALWAYS MUST BE) EPHEMERAL.
 *
 * @author Roman Levytskyi
 */
public final class ConsulSyncMap<K, V> extends ConsulMap<K, V> implements Map<K, V> {

  private final static Logger log = LoggerFactory.getLogger(ConsulSyncMap.class);

  private final KeyValueOptions kvOptions;
  private long timeout = 1500;

  public ConsulSyncMap(String name, String nodeId, Vertx vx, ConsulClient cC, String sessionId) {
    super(name, nodeId, vx, cC);
    // sync map's node mode should be EPHEMERAL, as lifecycle of its entries as long as verticle's.
    this.kvOptions = new KeyValueOptions().setAcquireSession(sessionId);
  }

  @Override
  public int size() {
    return toSync(consulKeys().compose(list -> Future.succeededFuture(list.size())), timeout);
  }

  @Override
  public boolean isEmpty() {
    return toSync(consulKeys().compose(list -> Future.succeededFuture(list.isEmpty())), timeout);
  }

  @Override
  public boolean containsKey(Object key) {
    return toSync(entries().compose(kvMap -> Future.succeededFuture(kvMap.keySet().contains(key))), timeout);
  }

  @Override
  public boolean containsValue(Object value) {
    return toSync(entries().compose(kvMap -> Future.succeededFuture(kvMap.values().contains(value))), timeout);
  }

  @Override
  public V get(Object key) {
    return toSync(getValue((K) key), timeout);
  }

  @Override
  public V put(K key, V value) {
    return toSync(putValue(key, value, kvOptions).compose(aBoolean -> {
      if (aBoolean) return Future.succeededFuture(value);
      else return Future.failedFuture("[" + nodeId + "]" + " failed to put KV: " + key + " -> " + value);
    }), timeout);
  }

  @Override
  public V remove(Object key) {
    return toSync(getValue((K) key).compose(v -> {
      if (v == null) return Future.succeededFuture();
      else {
        return delete((K) key).compose(aBoolean -> {
          if (aBoolean) return Future.succeededFuture(v);
          else return Future.failedFuture("[" + nodeId + "]" + " failed to remove an entry by K: " + key);
        });
      }
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

  @Override
  public Set<K> keySet() {
    return toSync(entries().compose(kvMap -> Future.succeededFuture(kvMap.keySet())), timeout);
  }

  @Override
  public Collection<V> values() {
    return toSync(entries().compose(kvMap -> Future.succeededFuture(kvMap.values())), timeout);
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return toSync(entries().compose(kvMap -> Future.succeededFuture(kvMap.entrySet())), timeout);
  }
}
