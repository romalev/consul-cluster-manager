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
import io.vertx.ext.consul.*;

import java.util.*;
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
    private static final String COMMON_NODE_TAG = "Vertx-Consul-Man";

    private Vertx vertx;
    private ConsulClient consulClient;

    private ServiceOptions serviceOptions;
    private ConsulClientOptions consulClientOptions;

    private volatile boolean active;
    private final String nodeId;
    private NodeListener nodeListener;

    public ConsulClusterManager(ServiceOptions serviceOptions) {
        log.trace("Initializing ConsulClusterManager with serviceOptions: '{}' by using default ConsulClientOptions.", serviceOptions.toJson().encodePrettily());
        this.serviceOptions = serviceOptions;
        consulClientOptions = new ConsulClientOptions();
        this.nodeId = UUID.randomUUID().toString();
        serviceOptions.setId(nodeId);
        serviceOptions.setName(buildServiceName(serviceOptions.getName(), nodeId));
        addTag(serviceOptions, COMMON_NODE_TAG);
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
        addTag(serviceOptions, COMMON_NODE_TAG);
    }

    private void init() {
        log.trace("Initializing the consul client...");
        consulClient = ConsulClient.create(vertx);
        registerWatcher();
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
    public <K, V> Map<K, V> getSyncMap(String name) {
        log.trace("Getting sync map by name: '{}'", name);
        return new HashMap<>();
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


    // FIX ME : doesn't really work :(
    @Override
    public List<String> getNodes() {
        // so far we actually grab a list of registered services within entire datacenter.
        log.trace("Getting all the nodes -> i.e. all registered service within entire consul dc...");
        Future<List<String>> future = Future.future();
        consulClient.catalogServices(result -> {
            if (result.succeeded()) {
                List<String> nodeIds = result.result().getList().stream()
                        .filter(service -> service.getTags().contains(COMMON_NODE_TAG))
                        .map(service -> getNodeIdOutOfServiceName(service.getName()))
                        .collect(Collectors.toList());
                log.trace("Service catalog -> listing ids: '{}'", nodeIds);
                future.complete(nodeIds);
            } else {
                log.error("Couldn't catalog available services due to: '{}'", result.cause().getMessage());
                future.fail(result.cause());
            }
        });
        return future.result();
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
    }

    @Override
    public synchronized void join(Handler<AsyncResult<Void>> resultHandler) {
        log.trace("'{}' is trying to join the cluster.", serviceOptions.getId());
        if (!active) {
            active = true;
            consulClient.registerService(serviceOptions, resultHandler);
        } else {
            log.warn("'{}' is NOT active.", serviceOptions.getId());
        }
    }

    @Override
    public synchronized void leave(Handler<AsyncResult<Void>> resultHandler) {
        log.trace("'{}' is trying to leave the cluster.", serviceOptions.getId());
        if (active) {
            active = false;
            nodeListener.nodeLeft(nodeId);
            consulClient.deregisterService(serviceOptions.getId(), resultHandler);
        } else {
            log.warn("'{}' is NOT active.", serviceOptions.getId());
        }
    }

    @Override
    public boolean isActive() {
        return active;
    }

    private void registerWatcher() {
        log.trace("Watch registration.");
        Watch.services(vertx).setHandler(watcher -> {
            if (watcher.succeeded()) {
                watcher.nextResult().getList().stream().filter(service -> service.getTags().contains(COMMON_NODE_TAG)).forEach(service -> {
                    log.trace("Watcher for service: '{}' has been registered.", nodeId);
                    nodeListener.nodeAdded(nodeId);
                });
            } else {
                log.warn("Couldn't register watcher for service: '{}'. Details: '{}'", nodeId, watcher.cause().getMessage());
            }
        }).start();
    }

    private void addTag(ServiceOptions options, String tagToBeAdded) {
        List<String> currentTags = options.getTags();
        List<String> newTags = new ArrayList<>(currentTags);
        newTags.add(tagToBeAdded);
        options.setTags(newTags);
    }

    private String buildServiceName(String serviceName, String nodeId) {
        return serviceName + "[" + nodeId + "]";
    }

    private String getNodeIdOutOfServiceName(String serviceName) {
        return serviceName.substring(serviceName.lastIndexOf('[') + 1, serviceName.length() - 1);
    }
}
