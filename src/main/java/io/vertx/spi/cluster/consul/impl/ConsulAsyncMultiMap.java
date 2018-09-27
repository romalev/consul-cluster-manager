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
import io.vertx.spi.cluster.consul.impl.cache.CacheManager;
import io.vertx.spi.cluster.consul.impl.cache.CacheMultiMap;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.vertx.spi.cluster.consul.impl.ConversationUtils.encodeF;

/**
 * Distributed consul async multimap implementation. IMPORTANT: purpose of async multimap in vertx cluster management is to hold mapping between
 * event bus names and its actual subscribers (subscriber is simply an entry containing host and port). When a message is fired from producer through
 * event bus to particular address (which is simple string), first - address gets resolved by cluster manager by looking up a key which is event bus address and then
 * getting one or set of actual IP addresses (key's values) where a message is going to fetchEventBusSubs routed to.
 * <p>
 * <b>Implementation details:</b>
 * <p>
 * - Consul itself doesn't provide out-of-the box the multimap implementation - this is (to be) addressed locally.
 * Entries of vertx event-bus subscribers MUST BE EPHEMERAL (AsyncMultiMap holds the subscribers) so node id is sort of appended to each key of this map.
 * Example :
 * __vertx.subs/users.create.channel/{nodeId} -> {nodeId} - localhost:5501
 * __vertx.subs/users.create.channel/{nodeId} -> {nodeId} - localhost:5502
 * __vertx.subs/users.push.sms.channel/{nodeId} -> {nodeId} - localhost:5505
 * __vertx.subs/users.push.sms.channel/{nodeId} -> {nodeId} - localhost:5506
 *
 * Note : https://github.com/vert-x3/vertx-consul-client/issues/54
 *
 * @author Roman Levytskyi
 */
public class ConsulAsyncMultiMap<K, V> extends ConsulMap<K, V> implements AsyncMultiMap<K, V> {

    private final static Logger log = LoggerFactory.getLogger(ConsulAsyncMultiMap.class);

    private final Vertx vertx;
    private final String nodeId;
    private final KeyValueOptions kvOpts;
    private final CacheMultiMap<K, V> cache;

    public ConsulAsyncMultiMap(String name, Vertx vertx, ConsulClient cC, CacheManager cM, String sessionId, String nodeId) {
        super(name, cC);
        this.vertx = vertx;
        this.nodeId = nodeId;
        this.cache = cM.createAndGetCacheMultiMap(name);
        // options to make entries of this map ephemeral.
        this.kvOpts = new KeyValueOptions().setAcquireSession(sessionId);
        // TODO: remove it.
        vertx.setPeriodic(15000, event -> log.trace("CacheMultiMap is : '{}'", cache));
    }

    @Override
    public void add(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
        assertKeyAndValueAreNotNull(k, v)
                .compose(aVoid -> encodeF(k, v))
                .compose(value -> putConsulValue(nodeKeyPath(k.toString()), value, kvOpts))
                .compose(putSucceeded -> putSucceeded ? cacheablePut(k, v) : Future.failedFuture(k.toString() + " wasn't added to consul kv store."))
                .setHandler(completionHandler);
    }

    @Override
    public void get(K k, Handler<AsyncResult<ChoosableIterable<V>>> asyncResultHandler) {
        assertKeyIsNotNull(k)
                .compose(aVoid -> cacheableGet(k))
                .setHandler(asyncResultHandler);
    }

    @Override
    public void remove(K k, V v, Handler<AsyncResult<Boolean>> completionHandler) {
        assertKeyAndValueAreNotNull(k, v)
                .compose(aVoid -> eventBusSubs(Optional.of(k.toString())))
                .compose(subs -> {
                    Future<Boolean> future = Future.future();
                    if (subs.isEmpty()) future.complete(false);
                    if (subs.contains(v)) {
                        Optional<ClusterNodeInfo> clusterNodeInfo = getClusterNodeInfo(v);
                        if (clusterNodeInfo.isPresent()) return cacheableRemove(k, v, clusterNodeInfo.get().nodeId);
                        else future.complete(false);
                    } else future.complete(false);
                    return future;
                })
                .setHandler(completionHandler);
    }

    @Override
    public void removeAllForValue(V v, Handler<AsyncResult<Void>> completionHandler) {
        removeAllMatching(v::equals, completionHandler);
    }

    @Override
    public void removeAllMatching(Predicate<V> p, Handler<AsyncResult<Void>> completionHandler) {
        fetchEventBusSubs(Optional.empty())
                .compose(keyValues -> {
                    List<Future> futures = new ArrayList<>();
                    keyValues.forEach(keyValue -> {
                        try {
                            ConversationUtils.GenericEntry<K, V> entry = ConversationUtils.decode(keyValue.getValue());
                            if (p.test(entry.getValue())) {
                                Optional<ClusterNodeInfo> clusterNodeInfo = getClusterNodeInfo(entry.getValue());
                                clusterNodeInfo.ifPresent(nodeInfo -> futures.add(cacheableRemove(entry.getKey(), entry.getValue(), nodeInfo.nodeId)));
                            }
                        } catch (Exception e) {
                            futures.add(Future.failedFuture(e));
                        }
                    });
                    return CompositeFuture.all(futures).compose(compositeFuture -> Future.<Void>succeededFuture());
                })
                .setHandler(completionHandler);
    }

    /**
     * Puts an entry to the cache.
     */
    private Future<Void> cacheablePut(K key, V value) {
        cache.put(key, value);
        return Future.succeededFuture();
    }


    /**
     * Gets entries (subscribers) either from cache (if it is NOT empty) or from consul kv store.
     */
    private Future<ChoosableIterable<V>> cacheableGet(K key) {
        if (cache.isEmpty()) {
            return eventBusChoosableSubs(key.toString())
                    .compose(vs -> {
                        // immediately update the internal cache.
                        cache.putAll(key, (ChoosableSet<V>) vs);
                        return Future.succeededFuture(vs);
                    });
        }
        return Future.succeededFuture(cache.get(key));
    }

    /**
     * Removes an entry (subscriber) from the cache only if it was already removed from consul kv store.
     * Note: we don't wait for watch REMOVE event which will be emitted.
     */
    private Future<Boolean> cacheableRemove(K key, V value, String nodeId) {
        String nodeKeyPath = nodeKeyPath(key.toString(), nodeId);
        return removeConsulValue(nodeKeyPath)
                .compose(removeSucceeded -> {
                    // immediately update the internal cache.
                    cache.remove(key, value);
                    return Future.succeededFuture(true);
                });
    }

    /**
     * Future choosable set of subscribers of eventBusAddress.
     */
    private Future<ChoosableIterable<V>> eventBusChoosableSubs(String eventBusAddress) {
        return eventBusSubs(Optional.of(eventBusAddress))
                .compose(subs -> {
                    Future<ChoosableIterable<V>> eventBusSubs = Future.future();
                    ChoosableSet<V> choosableIterable = new ChoosableSet<>(subs.size());
                    subs.forEach(choosableIterable::add);
                    eventBusSubs.complete(choosableIterable);
                    return eventBusSubs;
                });
    }

    /**
     * Future set of subscribers of eventBusAddress.
     */
    private Future<Set<V>> eventBusSubs(Optional<String> eventBusAddress) {
        return fetchEventBusSubs(eventBusAddress)
                .compose(keyValues -> {
                    Future<Set<V>> eventBusSubs = Future.future();
                    Set<String> encodedSubs = keyValues.stream().map(KeyValue::getValue).collect(Collectors.toSet());
                    Set<V> subs = new HashSet<>(encodedSubs.size()); //  O(1)
                    encodedSubs.forEach(s -> {
                        try {
                            ConversationUtils.GenericEntry<K, V> entry = ConversationUtils.decode(s);
                            subs.add(entry.getValue());
                        } catch (Exception e) {
                            log.error("Can't decode subscriber of: '{}' due to: '{}'", eventBusAddress.get(), e.getCause());
                        }
                    });
                    eventBusSubs.complete(subs);
                    return eventBusSubs;
                });
    }

    /**
     * Fetches "all" event bus subscribers if @address is empty, all subs that are subscribed to @address otherwise.
     */
    private Future<List<KeyValue>> fetchEventBusSubs(Optional<String> address) {
        Future<List<KeyValue>> future = Future.future();
        String consulKey = address.map(this::addressKeyPath).orElse(name);
        consulClient.getValues(consulKey, rHandler -> {
            if (rHandler.failed()) future.fail(rHandler.cause());
            else future.complete(nullSafeListResult(rHandler.result()));
        });
        return future;

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
     * Converts already decoded value (which is essentially an instance of {@link ClusterNodeInfo}.)
     */
    private Optional<ClusterNodeInfo> getClusterNodeInfo(V val) {
        return val.getClass() == ClusterNodeInfo.class ? Optional.of((ClusterNodeInfo) val) : Optional.empty();
    }
}