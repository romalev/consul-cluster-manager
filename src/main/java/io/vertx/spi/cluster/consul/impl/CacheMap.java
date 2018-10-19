package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.consul.KeyValueList;
import io.vertx.ext.consul.Watch;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static io.vertx.spi.cluster.consul.impl.ConversationUtils.asConsulEntry;


/**
 * Implementation of local IN-MEMORY cache which is essentially concurrent hash map under the hood.
 * Now:
 * Cache read operations happen synchronously by simply reading from {@link java.util.concurrent.ConcurrentHashMap}.
 * Cache WRITE operations happen either:
 * - through consul watch that monitors the consul kv store for updates (see https://www.consul.io/docs/agent/watches.html).
 * - when consul agent acknowledges the success of write operation from local vertx node (local node's data gets immediately cached without even waiting for a watch to take place.)
 * Note: local cache updates still might kick in through consul watch in case update succeeded in consul agent but wasn't yet acknowledged back to node. Eventually last write wins.
 *
 * @author Roman Levytskyi
 */
final class CacheMap<K, V> implements Map<K, V>, ConsulKvListener {

  private static final Logger log = LoggerFactory.getLogger(CacheMap.class);

  private final Watch<KeyValueList> watch;
  private final String name;

  private Map<K, V> cache = new ConcurrentHashMap<>();

  /**
   * @param name  - cache's name -> should always correspond to the same map's name to which cache is applied.
   * @param watch - consul watch.
   * @param map   - optional: cache can be initialized with already pre-built one.
   */
  CacheMap(String name, Watch<KeyValueList> watch, Optional<Map<K, V>> map) {
    this.name = name;
    this.watch = watch;
    map.ifPresent(kvMap -> cache.putAll(map.get()));
    start();
  }

  /**
   * Start caching data.
   */
  private void start() {
    log.trace("Cache for: " + name + " has been started.");
    watch.setHandler(kvWatchHandler()).start();
  }

  /**
   * Evicts the cache.
   */
  private void evict() {
    cache.clear();
  }

  @Override
  public int size() {
    return cache.size();
  }

  @Override
  public boolean isEmpty() {
    return cache.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return cache.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return cache.containsValue(value);
  }

  /**
   * Returns an actual cached data.
   */
  public V get(Object key) {
    return cache.get(key);
  }

  @Override
  public V put(K key, V value) {
    return cache.put(key, value);
  }

  @Override
  public V remove(Object key) {
    return cache.remove(key);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    cache.putAll(m);
  }

  @Override
  public void clear() {
    evict();
  }

  @Override
  public Set<K> keySet() {
    return cache.keySet();
  }

  @Override
  public Collection<V> values() {
    return cache.values();
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return cache.entrySet();
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
        cache.remove(entry.getKey());
        break;
      default:
        break;
    }
  }
}
