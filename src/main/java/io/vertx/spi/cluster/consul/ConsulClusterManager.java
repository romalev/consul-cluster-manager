package io.vertx.spi.cluster.consul;

import io.vertx.core.*;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.ext.consul.ServiceOptions;
import io.vertx.ext.consul.Watch;
import io.vertx.spi.cluster.consul.impl.ConsulAsyncMap;
import io.vertx.spi.cluster.consul.impl.ConsulAsyncMultiMap;
import io.vertx.spi.cluster.consul.impl.ConsulClusterManagerOptions;
import io.vertx.spi.cluster.consul.impl.ConsulSyncMap;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Cluster manager that uses Consul. See README for more details.
 * <p>
 * Things are still in progress.
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

    private Vertx vertx;
    private ConsulClient consulClient;
    private ServiceOptions serviceOptions;
    private ConsulClientOptions consulClientOptions;

    private volatile boolean active;

    private NodeListener nodeListener;
    private final String nodeId;
    private List<String> nodes;

    private ConsulSyncMap haInfoMap;

    private final Map<String, AsyncMap<?, ?>> asyncMapCache = new ConcurrentHashMap<>();
    private final Map<String, AsyncMultiMap<?, ?>> asyncMultiMapCache = new ConcurrentHashMap<>();

    public ConsulClusterManager(final ConsulClusterManagerOptions options) {
        this.serviceOptions = options.getServiceOptions();
        this.consulClientOptions = options.getClientOptions();
        this.nodeId = options.getNodeId();
    }

    private void init() {
        log.trace("Initializing the consul client...");
        consulClient = ConsulClient.create(vertx);
        initNodes();
        haInfoMap = new ConsulSyncMap(HA_INFO_MAP, vertx, consulClient);
    }

    @Override
    public void setVertx(Vertx vertx) {
        log.trace("Initializing the consul manager's vertx instance...");
        this.vertx = vertx;
        init();
    }

    @Override
    public <K, V> void getAsyncMultiMap(String name, Handler<AsyncResult<AsyncMultiMap<K, V>>> asyncResultHandler) {
        log.trace("Getting async multimap by name: '{}'", name);
        // consider getting async map within pure vertx event loop thread.
        vertx.executeBlocking(event -> {
            AsyncMultiMap asyncMultiMap = asyncMultiMapCache.computeIfAbsent(name, key -> new ConsulAsyncMultiMap<>(name, vertx, consulClient));
            event.complete(asyncMultiMap);
        }, asyncResultHandler);
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
    public <K, V> void getAsyncMap(String name, Handler<AsyncResult<AsyncMap<K, V>>> asyncResultHandler) {
        log.trace("Getting async map by name: '{}'", name);
        // consider getting async map within pure vertx event loop thread.
        vertx.executeBlocking(event -> {
            AsyncMap asyncMap = asyncMapCache.computeIfAbsent(name, key -> new ConsulAsyncMap<>(name, vertx, consulClient));
            event.complete(asyncMap);
        }, asyncResultHandler);
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
        return nodes;
    }

    @Override
    public void nodeListener(NodeListener listener) {
        log.trace("Initializing the node listener...");
        /*
         * 1. Whenever a node joins or leaves the cluster the registered NodeListener (if any) MUST be called with the
         * appropriate join or leave event.
         * 2. For all nodes that are part of the cluster, the registered NodeListener MUST be called with the exact same
         * sequence of join and leave events on all nodes.
         * 3. For any particular join or leave event that is handled in any NodeListener, anywhere in the cluster, the List
         * of nodes returned by getNodes must be identical.
         */
        this.nodeListener = listener;
        registerWatcher();
    }

    @Override
    public synchronized void join(Handler<AsyncResult<Void>> resultHandler) {
        Future<Void> future = Future.future();
        log.trace("'{}' is trying to join the cluster.", nodeId);
        if (!active) {
            active = true;
            consulClient.registerService(serviceOptions, result -> {
                if (result.succeeded()) {
                    log.trace("'{}' has joined the Consul cluster.");
                    future.complete();
                } else {
                    log.error("'{}' couldn't join the Consul cluster due to: {}", nodeId, result.cause().toString());
                    future.fail(result.cause());
                }
            });
        } else {
            log.warn("'{}' is NOT active.", serviceOptions.getId());
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
            consulClient.deregisterService(serviceOptions.getId(), event -> {
                if (event.succeeded()) {
                    log.trace("'{}': has left from the Consul cluster.", nodeId);
                    resultFuture.succeeded();
                } else {
                    log.error("'{}' couldn't leave the Consul cluster due to: '{}'", nodeId, event.cause().toString());
                    resultFuture.fail(event.cause());
                }
            });
        } else {
            log.warn("'{}' is NOT active.", serviceOptions.getId());
            resultFuture.complete();
        }
        resultFuture.setHandler(resultHandler);
    }

    @Override
    public boolean isActive() {
        return active;
    }

    // tricky !!! watchers are always executed  within the event loop context !!!
    // nodeAdded() call muset NEVER be called within event loop context !!!.
    private void registerWatcher() {
        // Executor watcherThreadExecutor = Executors.newFixedThreadPool(5);
        Watch.services(vertx).setHandler(event -> {
            if (event.succeeded()) {
                vertx.executeBlocking(blockingEvent -> {
                    event.nextResult().getList().stream()
                            .filter(service -> service.getTags().contains(ConsulClusterManagerOptions.getCommonNodeTag()))
                            .map(service -> getNodeIdOutOfServiceName(service.getName()))
                            .filter(receivedNodeId -> !receivedNodeId.equals(nodeId))
                            .forEach(newNodeId -> {
                                nodes.add(newNodeId);
                                log.trace("Adding new nodeId: '{}' to nodeListener.", newNodeId);
                                nodeListener.nodeAdded(newNodeId);
                            });
                    blockingEvent.complete();
                }, result -> {
                });
            } else {
                log.error("Couldn't register watcher for service: '{}'. Details: '{}'", nodeId, event.cause().getMessage());
            }
        }).start();
    }

    /**
     * note: blocking call.
     */
    private void initNodes() {
        // so far we actually grab a list of registered services within entire datacenter.
        log.trace("Getting all the nodes -> i.e. all registered service within entire consul dc...");

        CompletableFuture<List<String>> completableFuture = new CompletableFuture<>();
        consulClient.catalogServices(event -> {
            if (event.failed()) {
                log.error("Couldn't catalog services due to: {}", event.cause().toString());
                completableFuture.completeExceptionally(event.cause());
            } else {
                List<String> nodes = event.result().getList()
                        .stream()
                        .filter(service -> service.getTags().contains(ConsulClusterManagerOptions.getCommonNodeTag()))
                        .map(service -> getNodeIdOutOfServiceName(service.getName()))
                        .collect(Collectors.toList());
                completableFuture.complete(nodes);
            }
        });
        try {
            nodes = completableFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new VertxException(e);
        }
        log.trace("Node are: '{}'", nodes);
    }


    private String getNodeIdOutOfServiceName(String serviceName) {
        return serviceName.substring(serviceName.lastIndexOf('[') + 1, serviceName.length() - 1);
    }
}
