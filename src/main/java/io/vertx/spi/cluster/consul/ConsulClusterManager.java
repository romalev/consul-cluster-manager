package io.vertx.spi.cluster.consul;

import io.reactivex.Observable;
import io.reactivex.schedulers.Schedulers;
import io.vertx.core.AsyncResult;
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
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.ext.consul.ServiceOptions;
import io.vertx.reactivex.ext.consul.ConsulClient;
import io.vertx.reactivex.ext.consul.Watch;
import io.vertx.spi.cluster.consul.impl.ConsulSyncMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

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
    private static final String COMMON_NODE_TAG = "vertx-consul-clustering";
    private final String nodeId;
    private Vertx vertx;
    private io.vertx.reactivex.core.Vertx rxVertx;
    private ConsulClient consulClient;
    private ServiceOptions serviceOptions;
    private ConsulClientOptions consulClientOptions;
    private volatile boolean active;
    private NodeListener nodeListener;
    private ConsulSyncMap consulSyncMap;

    public ConsulClusterManager(ServiceOptions serviceOptions) {
        log.trace("Initializing ConsulClusterManager with serviceOptions: '{}' by using default ConsulClientOptions.", serviceOptions.toJson().encodePrettily());
        this.serviceOptions = serviceOptions;
        consulClientOptions = new ConsulClientOptions();
        this.nodeId = UUID.randomUUID().toString();
        serviceOptions.setId(nodeId);
        serviceOptions.setName(buildServiceName(serviceOptions.getName(), nodeId));
        addTag(serviceOptions);
    }

    public ConsulClusterManager(ServiceOptions serviceOptions, ConsulClientOptions clientOptions) {
        log.trace("Initializing ConsulClusterManager with serviceOptions: '{}'. ConsulClientOptions are: '{}'.",
                serviceOptions.toJson().encodePrettily(),
                clientOptions.toJson().encode());
        this.serviceOptions = serviceOptions;
        this.consulClientOptions = clientOptions;
        this.nodeId = UUID.randomUUID().toString();
        serviceOptions.setId(nodeId);
        serviceOptions.setName(buildServiceName(serviceOptions.getName(), nodeId));
        // is it safe ??? so far just a dummy implementation.
        addTag(serviceOptions);
    }

    private void init() {
        log.trace("Initializing the consul client...");
        this.rxVertx = io.vertx.reactivex.core.Vertx.newInstance(vertx);
        consulClient = ConsulClient.create(rxVertx);
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
    }

    @Override
    public <K, V> void getAsyncMap(String name, Handler<AsyncResult<AsyncMap<K, V>>> asyncResultHandler) {
        log.trace("Getting async map by name: '{}'", name);
    }

    @Override
    public Map<String, String> getSyncMap(String name) {
        log.trace("Getting sync map by name: '{}'", name);
        return consulSyncMap;
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
        // so far we actually grab a list of registered services within entire datacenter.
        log.trace("Getting all the nodes -> i.e. all registered service within entire consul dc...");

        List<String> nodeIds = consulClient.rxCatalogServices()
                .toObservable()
                .flatMap(serviceList -> Observable.fromIterable(serviceList.getList()))
                .filter(service -> service.getTags().contains(COMMON_NODE_TAG))
                .map(service -> getNodeIdOutOfServiceName(service.getName()))
                .doOnNext(s -> log.trace("Received: '{}' from Consul.", s))
                .toList()
                .doOnError(throwable -> log.error("Error occurred while getting services: '{}'", throwable.getMessage()))
                .blockingGet();
        return nodeIds;
    }

    @Override
    public void nodeListener(NodeListener listener) {
        log.trace("Initializing the node listener...");
        // TODO:
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
        vertx.executeBlocking(future -> {
            log.trace("'{}' is trying to join the cluster.", serviceOptions.getId());
            if (!active) {
                consulSyncMap = ConsulSyncMap.getInstance(rxVertx, consulClient);
                active = true;
                consulClient.registerService(serviceOptions, result -> {
                    if (result.succeeded()) {
                        future.complete();
                    } else {
                        future.fail(result.cause());
                    }
                });
            } else {
                log.warn("'{}' is NOT active.", serviceOptions.getId());
            }
        }, resultHandler);
    }

    @Override
    public synchronized void leave(Handler<AsyncResult<Void>> resultHandler) {
        log.trace("'{}' is trying to leave the cluster.", serviceOptions.getId());
        if (active) {
            active = false;
            consulClient.deregisterService(serviceOptions.getId(), resultHandler);
        } else {
            log.warn("'{}' is NOT active.", serviceOptions.getId());
        }
    }

    @Override
    public boolean isActive() {
        return active;
    }

    // tricky !!! watchers are always executed  within the event loop context !!!
    // nodeAdded() call muset NEVER be called within event loop context !!!.
    private void registerWatcher() {
        Executor watcherThreadExecutor = Executors.newFixedThreadPool(5);
        Watch.services(rxVertx).setHandler(event -> {
            if (event.succeeded()) {
                Observable.fromIterable(event.nextResult().getList())
                        .subscribeOn(Schedulers.from(watcherThreadExecutor))
                        .filter(service -> service.getTags().contains(COMMON_NODE_TAG))
                        .map(service -> getNodeIdOutOfServiceName(service.getName()))
                        .filter(receivedNodeId -> !receivedNodeId.equals(nodeId))
                        .doOnNext(newNodeId -> log.trace("New node: '{}' was added to consul", newNodeId))
                        .subscribe(
                                newNodeId -> {
                                    // not an event loop context since we subscribe the observable flow on vert.x-worker-thread (RxHelper.blockingScheduler(vertx))
                                    log.trace("Adding new nodeId: '{}' to nodeListener.", newNodeId);
                                    nodeListener.nodeAdded(newNodeId);
                                },
                                throwable -> log.error("Error occurred while processing new node ids in the cluster. Details: {}", throwable.getMessage()));
            } else {
                log.error("Couldn't register watcher for service: '{}'. Details: '{}'", nodeId, event.cause().getMessage());
            }
        }).start();
    }

    private void addTag(ServiceOptions options) {
        List<String> currentTags = options.getTags();
        List<String> newTags = new ArrayList<>(currentTags);
        newTags.add(ConsulClusterManager.COMMON_NODE_TAG);
        options.setTags(newTags);
    }

    private String buildServiceName(String serviceName, String nodeId) {
        return serviceName + "[" + nodeId + "]";
    }

    private String getNodeIdOutOfServiceName(String serviceName) {
        return serviceName.substring(serviceName.lastIndexOf('[') + 1, serviceName.length() - 1);
    }
}
