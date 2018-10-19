package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.*;
import io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ChoosableIterable;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueOptions;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.vertx.spi.cluster.consul.impl.ConversationUtils.asConsulEntry;
import static io.vertx.spi.cluster.consul.impl.ConversationUtils.asString_f;

/**
 * Distributed consul async multimap implementation. IMPORTANT: purpose of async multimap in vertx cluster management is to hold mapping between
 * event bus names and its actual subscribers (subscriber is simply an entry containing host and port). When a message is fired from producer through
 * event bus to particular address (which is simple string), first - address gets resolved by cluster manager by looking up a key which is event bus address and then
 * getting one or set of actual IP addresses (key's values) where a message is going to getSubs routed to.
 * <p>
 * <b>Implementation details:</b>
 * <p>
 * - Consul itself doesn't provide out-of-the box the multimap implementation - this is (to be) addressed locally.
 * Entries of vertx event-bus subscribers MUST BE EPHEMERAL (AsyncMultiMap holds the subscribers) so node id is sort of appended to each key of this map.
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
public class ConsulAsyncMultiMap<K, V> extends ConsulMap<K, V> implements AsyncMultiMap<K, V> {

  private final static Logger log = LoggerFactory.getLogger(ConsulAsyncMultiMap.class);

  private final KeyValueOptions kvOpts;
  private CacheMultiMap<K, V> cache;

  public ConsulAsyncMultiMap(String name, Vertx vertx, ConsulClient cC, CacheManager cM, String sessionId, String nodeId) {
    super(name, nodeId, vertx, cC);
    // options to make entries of this map ephemeral.
    this.kvOpts = new KeyValueOptions().setAcquireSession(sessionId);
    cache = cM.createAndGetCacheMultiMap(name, nodeId);
    // TODO: remove it.
    vertx.setPeriodic(15000, event -> log.trace("[" + nodeId + "]" + " CacheMultiMap is : " + cache));
  }

  @Override
  public void add(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
    assertKeyAndValueAreNotNull(k, v)
      .compose(aVoid -> getSubsByEbAddress(k.toString()))
      .compose(vs -> cacheablePut(k, vs, v))
      .setHandler(event -> vertx.runOnContext(action -> completionHandler.handle(event)));
  }

  @Override
  public void get(K k, Handler<AsyncResult<ChoosableIterable<V>>> asyncResultHandler) {
        /*
        Keeping subs cache in sync with what's stored in consul KV store is a little tricky.
        As entries are added or removed the ConsulKvListener will be called but when the node joins the cluster
        - it isn't provided the initial state via the ConsulKvListener therefore -> the first time map get is called for
        a subscription we *always* eagerly fetch the subs from Consul KV store, then consider that the initial state.
        In parallel we let the watch to monitor for updates -> see CacheMultiMap constructor - essentially last write wins.
        */
    assertKeyIsNotNull(k)
      .compose(aVoid -> cacheableGet(k))
      .setHandler(event -> vertx.runOnContext(action -> asyncResultHandler.handle(event)));
  }

  @Override
  public void remove(K k, V v, Handler<AsyncResult<Boolean>> completionHandler) {
    assertKeyAndValueAreNotNull(k, v)
      .compose(aVoid -> getSubsByEbAddress(k.toString()))
      .compose(subs -> {
        if (subs.isEmpty()) return Future.succeededFuture(false);
        if (subs.contains(v)) return cacheableRemove(k, v, getClusterNodeId(v));
        else return Future.succeededFuture(false);
      })
      .setHandler(completionHandler);
  }


  @Override
  public void removeAllForValue(V v, Handler<AsyncResult<Void>> completionHandler) {
    removeAllMatching(v::equals, completionHandler);
  }

  @Override
  public void removeAllMatching(Predicate<V> p, Handler<AsyncResult<Void>> completionHandler) {
    getSubs(Optional.empty())
      .compose(consulEntries -> {
        List<Future> futures = new ArrayList<>();
        consulEntries.forEach(kSetConsulEntry -> {
          kSetConsulEntry.getValue().forEach(v -> {
            if (p.test(v)) {
              futures.add(cacheableRemove(kSetConsulEntry.getKey(), v, Optional.of(kSetConsulEntry.getNodeId())));
            }
          });
        });
        return CompositeFuture.all(futures).compose(compositeFuture -> Future.<Void>succeededFuture());

      }).setHandler(completionHandler);
  }


  private Future<Void> cacheablePut(K k, Set<V> subs, V sub) {
    Set<V> newOne = new HashSet<>(subs);
    newOne.add(sub);
    return put(k, newOne)
      .compose(aBoolean -> {
        cache.put(k, sub);
        return Future.succeededFuture();
      });

  }

  /**
   * Gets entries (subscribers) either from cache (if it is NOT empty) or from consul kv store.
   */
  private Future<ChoosableIterable<V>> cacheableGet(K key) {
    Future<ChoosableIterable<V>> future = Future.future();
    if (cache.containsKey(key)) future.complete(cache.get(key));
    else {
      getSubsByEbAddress(key.toString()).setHandler(subsEvent -> {
        // immediately update the internal cache.
        ChoosableIterable<V> choosableIterable = toChoosable(subsEvent.result());
        cache.putAllForKey(key, (ChoosableSet<V>) choosableIterable);
        future.complete(choosableIterable);
      });
    }
    return future;
  }

  /**
   * Removes an entry (subscriber) from consul KV store (and from the internal cache only if it was already removed from consul kv store).
   * Note: we don't wait for watch REMOVE event which will be emitted.
   */
  private Future<Boolean> cacheableRemove(K key, V value, Optional<String> nodeId) {
    if (nodeId.isPresent()) {
      String keyPath = nodeKeyPath(key.toString(), nodeId.get());
      return getSubsByEbAddress(key.toString())
        .compose(vs -> {
          Set<V> subs = new HashSet<>(vs);
          subs.remove(value);
          if (subs.isEmpty()) {
            return removeConsulValue(keyPath)
              .compose(removeSucceeded -> {
                // immediately update the internal cache.
                cache.remove(key, value);
                return Future.succeededFuture(true);
              });
          } else {
            cache.remove(key, value);
            return put(key, subs);
          }
        });
    } else {
      String keyPath = addressKeyPath(key.toString());
      return removeConsulValues(keyPath).compose(event -> {
        // immediately update the internal cache.
        cache.remove(key, value);
        return Future.succeededFuture(true);
      });
    }
  }

  private Future<Boolean> put(K key, Set<V> vs) {
    return asString_f(key, vs, nodeId)
      .compose(encodedValue -> putConsulValue(nodeKeyPath(key.toString()), encodedValue, kvOpts));
  }

  /**
   * Fetches future set of subscribers of eventBusAddress.
   */
  private Future<Set<V>> getSubsByEbAddress(String eventBusAddress) {
    return getSubs(Optional.of(eventBusAddress))
      .compose(entries -> Future.succeededFuture(entries
        .stream()
        .map(ConsulEntry::getValue)
        .flatMap(Set::stream)
        .collect(Collectors.toSet())));
  }

  /**
   * Fetches "all" event bus subscribers if @address is empty, all subs that are subscribed to @address otherwise.
   * Event bus subs are being encoded so far.
   */
  private Future<Set<ConsulEntry<K, Set<V>>>> getSubs(Optional<String> address) {
    Future<Set<ConsulEntry<K, Set<V>>>> future = Future.future();
    String consulKey = address.map(this::addressKeyPath).orElse(name);
    consulClient.getValues(consulKey, rHandler -> {
      if (rHandler.failed()) future.fail(rHandler.cause());
      else {
        List<KeyValue> keyValues = nullSafeListResult(rHandler.result());
        Set<ConsulEntry<K, Set<V>>> resultSet = new HashSet<>();
        keyValues.forEach(keyValue -> {
          try {
            resultSet.add(asConsulEntry(keyValue.getValue()));
          } catch (Exception e) {
            future.fail(e);
          }
        });
        log.trace("[" + nodeId + "]" + " - fetched : + " + resultSet + " by address: " + consulKey);
        future.complete(resultSet);
      }
    });
    return future;

  }

  private ChoosableIterable<V> toChoosable(Set<V> set) {
    ChoosableSet<V> choosableSet = new ChoosableSet<>(set.size());
    set.forEach(choosableSet::add);
    return choosableSet;
  }

  /**
   * Builds a key used to access particular subscriber.
   *
   * @param address - refers to actual name of event bus i.e. - it's address.
   * @return key.
   */
  private String nodeKeyPath(String address) {
    return name + "/" + address + "/" + nodeId;
  }

  /**
   * Builds a key used to access particular subscriber.
   *
   * @param address - refers to actual name of event bus i.e. - it's address.
   * @param _nodeId - points to subscriber's node id.
   * @return key that is compatible with consul central KV store.
   */
  private String nodeKeyPath(String address, String _nodeId) {
    return name + "/" + address + "/" + _nodeId;
  }

  /**
   * Builds the key to access all subscribers by specific address.
   *
   * @param address - represent subscribers address.
   * @return key that is compatible with consul central KV store.
   */
  private String addressKeyPath(String address) {
    return name + "/" + address;
  }

  /**
   * Gets node id out of {@link ClusterNodeInfo} if value is instance of {@link ClusterNodeInfo}.
   */
  private Optional<String> getClusterNodeId(V val) {
    return val.getClass() == ClusterNodeInfo.class ? Optional.of(((ClusterNodeInfo) val).nodeId) : Optional.empty();
  }
}
