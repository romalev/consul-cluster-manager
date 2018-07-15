package io.vertx.spi.cluster.consul;

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
import io.vertx.spi.cluster.consul.impl.NodeJoiner;
import io.vertx.spi.cluster.consul.impl.NodeManager;
import io.vertx.spi.cluster.consul.impl.maps.ConsulAsyncMap;
import io.vertx.spi.cluster.consul.impl.maps.ConsulAsyncMultiMap;
import io.vertx.spi.cluster.consul.impl.maps.ConsulSyncMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
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

    private Vertx vertx;
    private ConsulClient consulClient;
    private NodeListener nodeListener;
    private NodeManager nodeManager;
    private NodeJoiner nodeJoiner;

    private volatile boolean active;

    private String nodeSessionId;

    private final String nodeId;
    private final ConsulClientOptions consulClientOptions;
    private final Map<String, AsyncMap<?, ?>> asyncMapCache = new ConcurrentHashMap<>();
    private final Map<String, AsyncMultiMap<?, ?>> asyncMultiMapCache = new ConcurrentHashMap<>();

    private ConsulSyncMap haInfoMap;

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
        this.nodeJoiner = new NodeJoiner(vertx, consulClient);
        this.nodeManager = new NodeManager(vertx, consulClientOptions, consulClient, nodeListener, nodeId, NODES_MAP);
        this.haInfoMap = new ConsulSyncMap(HA_INFO_MAP, vertx, consulClient, consulClientOptions);
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
        AsyncMultiMap asyncMultiMap = asyncMultiMapCache.computeIfAbsent(name, key -> new ConsulAsyncMultiMap<>(name, vertx, consulClient));
        futureMultiMap.complete(asyncMultiMap);
        futureMultiMap.setHandler(asyncResultHandler);
    }

    @Override
    public <K, V> void getAsyncMap(String name, Handler<AsyncResult<AsyncMap<K, V>>> asyncResultHandler) {
        log.trace("Getting async map by name: '{}'", name);
        Future<AsyncMap<K, V>> futureMap = Future.future();
        AsyncMap asyncMap = asyncMapCache.computeIfAbsent(name, key -> new ConsulAsyncMap<>(name, vertx, consulClient));
        futureMap.complete(asyncMap);
        futureMap.setHandler(asyncResultHandler);

    }

    @Override
    public <K, V> Map<K, V> getSyncMap(String name) {
        // name is not being used.
        log.trace("Getting sync map by name: '{}'", name);
        return haInfoMap;
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
        return nodeManager.getNodes();
    }

    @Override
    public void nodeListener(NodeListener listener) {
        log.trace("Initializing the node listener...");
        this.nodeListener = listener;
        nodeManager.listenForNewNodes().start();
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
        future.setHandler(resultHandler);
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
}