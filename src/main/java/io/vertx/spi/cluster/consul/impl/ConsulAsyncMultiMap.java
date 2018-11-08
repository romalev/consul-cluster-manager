package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.*;
import io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo;
import io.vertx.core.impl.TaskQueue;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ChoosableIterable;
import io.vertx.ext.consul.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static io.vertx.spi.cluster.consul.impl.ConversationUtils.*;

/**
 * Distributed consul async multimap implementation. IMPORTANT: purpose of async multimap in vertx cluster management is to hold mapping between
 * event bus names and its actual subscribers (subscriber is simply an entry containing host and port). When a message is fired from producer through
 * event bus to particular address (which is simple string), first - address gets resolved by cluster manager by looking up a key which is event bus address and then
 * getting one or set of actual IP addresses (key's values) where a message is going to getEntries routed to.
 * <p>
 * <b>Implementation details:</b>
 * <p>
 * - Consul itself doesn't provide out-of-the box the multimap implementation - this is (to be) addressed locally.
 * Entries of vertx event-bus subscribers MUST BE EPHEMERAL (AsyncMultiMap holds the subscribers) so node id is appended to each key of this map.
 * Example :
 * __vertx.subs/{address1}/{nodeId} -> Set<V>
 * __vertx.subs/{address1}/{nodeId} -> Set<V>
 * __vertx.subs/{address2}/{nodeId} -> Set<V>
 * __vertx.subs/{address3}/{nodeId} -> Set<V>
 * <p>
 * Note : https://github.com/vert-x3/vertx-consul-client/issues/54
 *
 * @author Roman Levytskyi
 */
public class ConsulAsyncMultiMap<K, V> extends ConsulMap<K, V> implements AsyncMultiMap<K, V>, KvListener {

  private final static Logger log = LoggerFactory.getLogger(ConsulAsyncMultiMap.class);

  private final TaskQueue taskQueue = new TaskQueue();
  private final KeyValueOptions kvOpts;
  private final boolean preferConsistency;
  /*
   * Implementation of local IN-MEMORY multimap cache which is essentially concurrent hash map under the hood.
   * Cache is enabled ONLY when {@code preferConsistency} is set to false i.e. availability (better latency) is preferred.
   * If cache is enabled:
   * Cache read operations happen synchronously by simply reading from {@link java.util.concurrent.ConcurrentHashMap}.
   * Cache WRITE operations happen either:
   * - through consul watch that monitors the consul kv store for updates (see https://www.consul.io/docs/agent/watches.html).
   * - when consul agent acknowledges the success of write operation (local node's data gets immediately cached without even waiting for a watch to take place.)
   * Note: local cache updates still might kick in through consul watch in case update succeeded in consul agent but wasn't yet acknowledged back to node. Eventually last write wins.
   */
  private ConcurrentMap<K, ChoosableSet<V>> cache;
  private Watch<KeyValueList> watch;

  public ConsulAsyncMultiMap(String name,
                             Vertx vertx,
                             ConsulClient cC,
                             ConsulClientOptions options,
                             String sessionId,
                             String nodeId,
                             boolean preferConsistency) {
    super(name, nodeId, vertx, cC);
    this.preferConsistency = preferConsistency;
    // options to make entries of this map ephemeral.
    this.kvOpts = new KeyValueOptions().setAcquireSession(sessionId);
    if (!preferConsistency) { // if cp is disabled then disable caching.
      cache = new ConcurrentHashMap<>();
      watch = Watch.keyPrefix(name, vertx, options);
      startWatching(watch);
    }
  }

  @Override
  public void add(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
    assertKeyAndValueAreNotNull(k, v)
      .compose(aVoid -> doGet(k))
      .compose(vs -> doPut(k, vs.getIds(), v))
      .setHandler(completionHandler);
  }

  @Override
  public void remove(K k, V v, Handler<AsyncResult<Boolean>> completionHandler) {
    assertKeyAndValueAreNotNull(k, v)
      .compose(aVoid -> doGet(k))
      .compose(vs -> delete(k, v, vs))
      .setHandler(completionHandler);
  }

  @Override
  public void removeAllForValue(V v, Handler<AsyncResult<Void>> completionHandler) {
    removeAllMatching(v::equals, completionHandler);
  }

  @Override
  public void removeAllMatching(Predicate<V> p, Handler<AsyncResult<Void>> completionHandler) {
    getEntriesFromConsulKv(Optional.empty())
      .compose(consulEntries -> {
        List<Future> futures = new ArrayList<>();
        consulEntries.forEach(kSetConsulEntry -> {
          kSetConsulEntry.getValue().forEach(v -> {
            if (p.test(v)) {
              futures.add(delete(kSetConsulEntry.getKey(), v, toChoosableSet(kSetConsulEntry.getValue())));
            }
          });
        });
        return CompositeFuture.all(futures).compose(compositeFuture -> Future.<Void>succeededFuture());
      }).setHandler(completionHandler);
  }

  @Override
  public void get(K k, Handler<AsyncResult<ChoosableIterable<V>>> resultHandler) {
    assertKeyIsNotNull(k)
      .compose(aVoid -> doGet(k))
      .compose(vs -> succeededFuture((ChoosableIterable<V>) vs))
      .setHandler(resultHandler);
  }


  private Future<Void> doPut(K k, Set<V> subs, V sub) {
    return preferConsistency ? nonCacheablePut(k, subs, sub) : cacheablePut(k, subs, sub);
  }

  private Future<Void> cacheablePut(K k, Set<V> subs, V sub) {
    return nonCacheablePut(k, subs, sub)
      .compose(aVoid -> {
        addEntryToCache(k, sub);
        return succeededFuture();
      });

  }

  private Future<Void> nonCacheablePut(K k, Set<V> subs, V sub) {
    Set<V> newOne = new HashSet<>(subs);
    newOne.add(sub);
    return putToConsulKv(k, newOne, nodeId)
      .compose(aBoolean -> aBoolean ? succeededFuture() : failedFuture(sub.toString() + ": wasn't added to: " + name));
  }

  private Future<Boolean> putToConsulKv(K key, Set<V> vs, String _nodeId) {
    return asFutureString(key, vs, _nodeId)
      .compose(encodedValue -> putConsulValue(keyPath(key.toString(), _nodeId), encodedValue, kvOpts));
  }

  /*
   * THIS IS BAD! We are wrapping async call into sync and execute it on the taskQueue. This way we maintain the order
   * in which "get" tasks are executed.
   * If we simply implement this method as : return preferConsistency ? nonCacheableGet(key) : cacheableGet(key);
   * then {@link ClusteredEventBusTest.sendNoContext} will fail due to the fact async calls to get subs by key are unordered.
   */
  private Future<ChoosableSet<V>> doGet(K key) {
    Future<ChoosableSet<V>> out = Future.future();
    VertxInternal vertxInternal = (VertxInternal) vertx;
    vertxInternal.getOrCreateContext().<ChoosableSet<V>>executeBlocking(event -> {
      Future<ChoosableSet<V>> future = preferConsistency ? nonCacheableGet(key) : cacheableGet(key);
      ChoosableSet<V> choosableSet = toSync(future, 5000);
      event.complete(choosableSet);
    }, taskQueue, res -> out.complete(res.result()));
    return out;
  }

  private Future<ChoosableSet<V>> cacheableGet(K key) {
    if (cache.containsKey(key)) return succeededFuture(cache.get(key));
    else return nonCacheableGet(key)
      .compose(vs -> {
        addEntriesToCache(key, vs);
        return succeededFuture(vs);
      });
  }

  private Future<ChoosableSet<V>> nonCacheableGet(K key) {
    return getEntriesByKey(key.toString()).compose(vs -> succeededFuture(toChoosableSet(vs)));
  }

  private Future<Set<V>> getEntriesByKey(String eventBusAddress) {
    return getEntriesFromConsulKv(Optional.of(eventBusAddress))
      .compose(entries -> succeededFuture(entries
        .stream()
        .map(ConsulEntry::getValue)
        .flatMap(Set::stream)
        .collect(Collectors.toSet())));
  }


  private Future<Boolean> delete(K key, V value, ChoosableSet<V> from) {
    return preferConsistency ? nonCacheableDelete(key, value, from) : cacheableDelete(key, value, from);
  }

  private Future<Boolean> cacheableDelete(K key, V value, ChoosableSet<V> from) {
    return nonCacheableDelete(key, value, from)
      .compose(aBoolean -> {
        if (aBoolean) {
          removeEntryFromCache(key, value);
        }
        return succeededFuture(aBoolean);
      });
  }

  private Future<Boolean> nonCacheableDelete(K key, V value, ChoosableSet<V> from) {
    Optional<String> clusterNodeId = getClusterNodeId(value);
    if (clusterNodeId.isPresent()) {
      if (from.remove(value)) {
        if (from.isEmpty()) {
          return deleteConsulValue(keyPath(key.toString(), clusterNodeId.get()));
        } else {
          return putToConsulKv(key, toHashSet(from), clusterNodeId.get());
        }
      } else {
        return Future.succeededFuture(false);
      }
    } else {
      // We're here entering a complicated scenario since actual Value is not type of {@link ClusterNodeInfo}
      // which means we can't extract node id that given Value belongs to.
      // Normally this should NOT HAPPEN in production code due to the fact {@link EventBus} operates ONLY AND ONLY with {@link ClusterNodeInfo}
      // So it's only done to satisfy TCK
      return getEntriesFromConsulKv(Optional.of(key.toString())).compose(consulEntries -> {
        List<Future> futures = new ArrayList<>();
        consulEntries.forEach(kSetConsulEntry -> {
          Set<V> subs = kSetConsulEntry.getValue();
          boolean removed = subs.remove(value);
          if (removed) {
            if (subs.isEmpty()) {
              futures.add(deleteConsulValue(keyPath(key.toString(), kSetConsulEntry.getNodeId())));
            } else {
              futures.add(putToConsulKv(key, subs, kSetConsulEntry.getNodeId()));
            }
          } else {
            futures.add(succeededFuture(false));
          }
        });
        return CompositeFuture.all(futures).map(compositeFuture -> {
          for (int i = 0; i < compositeFuture.size(); i++) {
            boolean resultAt = compositeFuture.resultAt(i);
            if (!resultAt) return false;
          }
          return true;
        });
      });
    }
  }

  private Future<Set<ConsulEntry<K, Set<V>>>> getEntriesFromConsulKv(Optional<String> address) {
    String consulKey = address.map(this::keyPath).orElse(name);
    Future<KeyValueList> future = Future.future();
    consulClient.getValues(consulKey, future.completer());

    return future.compose(keyValueList -> {
      List<KeyValue> keyValues = nullSafeListResult(keyValueList);
      List<Future> futures = new ArrayList<>();
      keyValues
        .stream()
        .filter(keyValue -> name.equals(consulKey) || extractKey(keyValue.getKey()).equals(consulKey))
        .forEach(keyValue -> futures.add(asFutureConsulEntry(keyValue.getValue())));

      return CompositeFuture.all(futures).map(compositeFuture -> {
        Set<ConsulEntry<K, Set<V>>> resultSet = new HashSet<>();
        for (int i = 0; i < compositeFuture.size(); i++) {
          resultSet.add(compositeFuture.resultAt(i));
        }
        return resultSet;
      });
    });
  }

  private ChoosableSet<V> toChoosableSet(Set<V> set) {
    ChoosableSet<V> choosableSet = new ChoosableSet<>(set.size());
    set.forEach(choosableSet::add);
    return choosableSet;
  }

  private Set<V> toHashSet(ChoosableSet<V> set) {
    Set<V> hashSet = new HashSet<>(set.size());
    set.forEach(hashSet::add);
    return hashSet;
  }

  // builds the key used to access particular subscriber - i.e. __vertx.subs/@address/@nodeId
  private String keyPath(String address, String _nodeId) {
    return name + "/" + address + "/" + _nodeId;
  }

  // builds the key to access all subscribers by specific address - i.e. __vertx.subs/@address
  private String keyPath(String address) {
    return name + "/" + address;
  }


  private String extractKey(String multimapKey) {
    return multimapKey.substring(0, multimapKey.lastIndexOf("/"));
  }

  /**
   * Gets node id out of {@link ClusterNodeInfo} if value is instance of {@link ClusterNodeInfo}.
   */
  private Optional<String> getClusterNodeId(V val) {
    return val.getClass() == ClusterNodeInfo.class ? Optional.of(((ClusterNodeInfo) val).nodeId) : Optional.empty();
  }

  private void addEntryToCache(K key, V value) {
    ChoosableSet<V> choosableSet = cache.get(key);
    if (choosableSet == null) choosableSet = new ChoosableSet<>(1);
    choosableSet.add(value);
    cache.put(key, choosableSet);
    log.trace("[" + nodeId + "]" + " Cache: " + name + " after put of " + key + " -> " + value + ": " + this.toString());
  }

  private void removeEntryFromCache(K key, V value) {
    ChoosableSet<V> choosableSet = cache.get(key);
    if (choosableSet == null) return;
    choosableSet.remove(value);
    if (choosableSet.isEmpty()) cache.remove(key);
    else cache.put(key, choosableSet);
    log.trace("[" + nodeId + "]" + " Cache: " + name + " after remove of " + key + " -> " + value + ": " + this.toString());
  }

  private void addEntriesToCache(K key, ChoosableSet<V> values) {
    cache.put(key, values);
  }

  @Override
  public void entryUpdated(EntryEvent event) {
    log.trace("[" + nodeId + "]" + " Entry: " + event.getEntry().getKey() + " is " + event.getEventType());
    ConsulEntry<K, Set<V>> entry;
    try {
      entry = asConsulEntry(event.getEntry().getValue());
    } catch (Exception e) {
      log.error("Failed to decode: " + event.getEntry().getKey() + " -> " + event.getEntry().getValue(), e);
      return;
    }
    switch (event.getEventType()) {
      case WRITE:
        entry.getValue().forEach(v -> addEntryToCache(entry.getKey(), v));
        break;
      case REMOVE:
        entry.getValue().forEach(v -> removeEntryFromCache(entry.getKey(), v));
        break;
      default:
        break;
    }
  }

  @Override
  public String toString() {
    return Json.encodePrettily(cache);
  }

  @Override
  public void close(Handler<AsyncResult<Void>> completionHandler) {
    if (cache != null) cache.clear();
    if (watch != null) watch.stop();
    Future.<Void>succeededFuture().setHandler(completionHandler);
  }
}
