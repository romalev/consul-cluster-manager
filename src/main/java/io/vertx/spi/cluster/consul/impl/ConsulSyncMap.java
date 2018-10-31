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
import java.util.concurrent.ConcurrentHashMap;

import static io.vertx.core.Future.succeededFuture;
import static io.vertx.spi.cluster.consul.impl.ConversationUtils.asConsulEntry;

/**
 * Distributed sync map implementation based on consul key-value store.
 * It is ONLY used by {@link io.vertx.core.impl.HAManager} - essentially it holds HA INFO about cluster node's vertices
 *
 * @author Roman Levytskyi
 */
public final class ConsulSyncMap<K, V> extends ConsulMap<K, V> implements Map<K, V>, ConsulKvListener {

  private final static Logger log = LoggerFactory.getLogger(ConsulSyncMap.class);

  private final Map<K, V> cache = new ConcurrentHashMap<>();
  private final KeyValueOptions kvOptions;
  private long timeout = 10000;

  public ConsulSyncMap(String name, String nodeId, Vertx vx, ConsulClient cC, String sessionId, CacheManager cM) {
    super(name, nodeId, vx, cC);
    // sync map's node mode should be EPHEMERAL, as lifecycle of its entries as long as verticle's.
    this.kvOptions = new KeyValueOptions().setAcquireSession(sessionId);
    listen(cM.createAndGetMapWatch(name));
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
    return toSync(putValue(key, value, kvOptions).compose(aBoolean -> {
      if (aBoolean) return succeededFuture(value);
      else return Future.failedFuture("[" + nodeId + "]" + " failed to put KV: " + key + " -> " + value);
    }), timeout);
  }

  @Override
  public V remove(Object key) {
    return toSync(getValue((K) key).compose(v -> {
      if (v == null) {
        return succeededFuture();
      } else {
        return delete((K) key).compose(aBoolean -> {
          if (aBoolean) return succeededFuture(v);
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

  @Override
  public void entryUpdated(EntryEvent event) {
    ConsulEntry<K, V> entry;
    try {
      entry = asConsulEntry(event.getEntry().getValue());
    } catch (Exception e) {
      log.error("Failed to decode: " + event.getEntry().getKey() + " -> " + event.getEntry().getValue(), e);
      return;
    }
    switch (event.getEventType()) {
      case WRITE:
        cache.put(entry.getKey(), entry.getValue());
        break;
      case REMOVE:
        // TODO: check is HA enabled
        // defer removing from cache - this is for HA to work correctly.
        vertx.setTimer(10000, handler -> cache.remove(entry.getKey()));
        break;
      default:
        break;
    }
  }
}
