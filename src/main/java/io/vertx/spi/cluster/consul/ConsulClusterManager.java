package io.vertx.spi.cluster.consul;

import com.google.common.collect.ImmutableSet;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.ext.consul.Check;
import io.vertx.ext.consul.CheckStatus;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.spi.cluster.consul.impl.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Cluster manager that uses Consul. Given implementation is fully based vertx consul client. See README for more details.
 * <p>
 * --
 * Notes :
 * 1) slf4j is used here instead of default vertx jul.
 * 2) there are lof trace messages now (to have a clue what's going on under the hood) -> this will get removed once the version
 * of given cluster manager more or less stable.
 * 3) java docs have to added.
 *
 * @author Roman Levytskyi
 */
public class ConsulClusterManager implements ClusterManager {

    private static final Logger log = LoggerFactory.getLogger(ConsulClusterManager.class);

    private static final String HA_INFO_MAP = "__vertx.haInfo";
    private static final String NODES_MAP = "__vertx.nodes";
    private static final String SUBS_MA = "__vertx.subs";

    // indicates names of the maps which entries are going to be locked by nodes.
    // particular map entry gets locked by node that has just created it.
    private static final Set<String> lockedMaps = ImmutableSet.of(HA_INFO_MAP, NODES_MAP, SUBS_MA);
    // represents id of consul session -> this is used to lock map entries.
    private String nodeSessionId;

    private Vertx vertx;
    private ConsulClient consulClient;
    private NodeListener nodeListener;
    private NodeDiscovery nodeDiscovery;
    private NodeJoiner nodeJoiner;

    private volatile boolean active;

    private Map haInfoCache;

    private final String nodeId;
    private final ConsulClientOptions consulClientOptions;
    private final Map<String, AsyncMap<?, ?>> asyncMapCache = new ConcurrentHashMap<>();
    private final Map<String, AsyncMultiMap<?, ?>> asyncMultiMapCache = new ConcurrentHashMap<>();

    public ConsulClusterManager(final ConsulClientOptions options) {
        Objects.requireNonNull(options, "Consul client options can't be null");
        this.consulClientOptions = options;
        this.nodeId = UUID.randomUUID().toString();

    }

    public ConsulClusterManager() {
        this.consulClientOptions = new ConsulClientOptions();
        this.nodeId = UUID.randomUUID().toString();
    }

    @Override
    public void setVertx(Vertx vertx) {
        log.trace("Injecting Vert.x instance and Initializing consul client ...");
        this.vertx = vertx;
        this.consulClient = ConsulClient.create(vertx, consulClientOptions);
        this.nodeJoiner = new NodeJoiner(vertx, consulClient, NODES_MAP);
        this.nodeDiscovery = new NodeDiscovery(vertx, consulClientOptions, consulClient, nodeListener, nodeId, NODES_MAP);
    }

    /**
     * Every eventbus handler has an ID. SubsMap (subscriber map) is a MultiMap which
     * maps handler-IDs with server-IDs and thus allows the eventbus to determine where
     * to send messages.
     *
     * @param name A unique name by which the the MultiMap can be identified within the cluster.
     * @return subscription map
     */
    @Override
    public <K, V> void getAsyncMultiMap(String name, Handler<AsyncResult<AsyncMultiMap<K, V>>> asyncResultHandler) {
        log.trace("Getting async multimap by name: '{}'", name);
        Future<AsyncMultiMap<K, V>> futureMultiMap = Future.future();
        AsyncMultiMap asyncMultiMap = asyncMultiMapCache.computeIfAbsent(name, key -> new ConsulAsyncMultiMap<>(name, vertx, consulClient, consulClientOptions, nodeSessionId));
        futureMultiMap.complete(asyncMultiMap);
        futureMultiMap.setHandler(asyncResultHandler);
    }

    @Override
    public <K, V> void getAsyncMap(String name, Handler<AsyncResult<AsyncMap<K, V>>> asyncResultHandler) {
        log.trace("Getting async map by name: '{}'", name);
        Future<AsyncMap<K, V>> futureMap = Future.future();
        AsyncMap asyncMap = asyncMapCache.computeIfAbsent(name, key -> new ConsulAsyncMap<>(name, vertx, consulClient, consulClientOptions, nodeSessionId));
        futureMap.complete(asyncMap);
        futureMap.setHandler(asyncResultHandler);

    }

    @Override
    public <K, V> Map<K, V> getSyncMap(String name) {
        // name is not being used.
        log.trace("Getting sync map by name: '{}'", name);
        boolean lock = lockedMaps.contains(name);
        return new ConsulSyncMap<K, V>(name, vertx, consulClient, consulClientOptions, nodeSessionId, haInfoCache);
    }

    @Override
    public void getLockWithTimeout(String name, long timeout, Handler<AsyncResult<Lock>> resultHandler) {
        log.trace("Getting lock with timeout by name: '{}'", name);
    }

    @Override
    public void getCounter(String name, Handler<AsyncResult<Counter>> resultHandler) {
        log.trace("Getting counter by name: '{}'", name);
    }

    @Override
    public String getNodeID() {
        log.trace("Getting node id: '{}'", nodeId);
        return nodeId;
    }

    @Override
    public List<String> getNodes() {
        return nodeDiscovery.getNodes();
    }

    @Override
    public void nodeListener(NodeListener listener) {
        log.trace("Initializing the node listener...");
        this.nodeListener = listener;
        // nodeDiscovery.listenForNewNodes().start();
    }

    @Override
    public synchronized void join(Handler<AsyncResult<Void>> resultHandler) {
        Future<Void> future = Future.future();
        log.trace("'{}' is trying to join the cluster.", nodeId);
        if (!active) {
            active = true;
            nodeJoiner.join(nodeId, registration -> {
                if (registration.succeeded()) {
                    log.trace("Node: '{}' has joined the cluster.", nodeId);
                    this.nodeSessionId = registration.result();
                    future.complete();
                } else {
                    log.error("Node: '{}' couldn't join the cluster due to: '{}'", nodeId, registration.cause().toString());
                    future.fail(registration.cause());
                }
            });

        } else {
            log.warn("'{}' is NOT active.", nodeId);
            future.complete();
        }

        // while joining the cluster joining node has to :
        // a: discover rest of the vertx nodes within the consul cluster and cache them locally.
        // b: receive HA information -> which is stored in __vertx.haInfo map within Consul KV store.
        future
                .compose(aVoid -> nodeDiscovery.discoverClusterNodes())
                .compose(aList -> initHaInfoCache())
                .compose(haInfoCache -> {
                    Future<Void> endFuture = Future.future();
                    this.haInfoCache = new ConcurrentHashMap();
                    this.haInfoCache.putAll(haInfoCache);
                    endFuture.complete();
                    return endFuture;
                }).setHandler(resultHandler);
    }

    @Override
    public synchronized void leave(Handler<AsyncResult<Void>> resultHandler) {
        Future<Void> resultFuture = Future.future();
        log.trace("'{}' is trying to leave the cluster.", nodeId);
        if (active) {
            active = false;

        } else {
            log.warn("'{}' is NOT active.", nodeId);
            resultFuture.complete();
        }
        resultFuture.setHandler(resultHandler);
    }

    @Override
    public boolean isActive() {
        return active;
    }

    /**
     * TODO: clean up ONLY & ONLY health checks assosiated with session id.
     */
    private void cleanFailingHealthChecks() {
        vertx.setPeriodic(15000, event -> {
            consulClient.localChecks(localChecks -> {
                List<Check> failedCheck = localChecks.result().stream().filter(check -> check.getStatus() == CheckStatus.CRITICAL).collect(Collectors.toList());
                failedCheck.forEach(check -> {
                    consulClient.deregisterCheck(check.getId(), checkDerRes -> {
                        if (checkDerRes.succeeded()) {
                            log.trace("Check: {} has been unregistered.", check.getId());
                        } else {
                            log.error("Can't unregister check: '{}' due to: '{}'", check.getId(), checkDerRes.cause().toString());
                        }
                    });
                });
            });
        });
    }

    private <K, V> Future<Map<K, V>> initHaInfoCache() {
        log.trace("Initializing: '{}' internal cache ... ", HA_INFO_MAP);
        Future<Map<K, V>> futureHaInfoCache = Future.future();
        consulClient.getValues(HA_INFO_MAP, futureMap -> {
            if (futureMap.succeeded()) {
                if (futureMap.result().getList() == null) {
                    futureHaInfoCache.complete(new ConcurrentHashMap<>());
                } else {
                    // TODO : is casting here sufficient here ???
                    Map<K, V> collectedMap = futureMap.result().getList().stream().collect(Collectors.toMap(o -> (K) o.getKey(), o -> (V) o.getValue()));
                    futureHaInfoCache.complete(collectedMap);
                }
            } else {
                log.trace("Can't initialize the : '{}' due to: '{}'", HA_INFO_MAP, futureMap.cause().toString());
                futureHaInfoCache.fail(futureMap.cause());
            }
        });
        return futureHaInfoCache;
    }
}