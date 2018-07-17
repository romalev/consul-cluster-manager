package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.Watch;

import java.util.List;

/**
 * Consul node map - discovers new vertx nodes within the consul cluster and keeps them locally in the cache.
 *
 * @author Roman Levytskyi
 */
public final class NodeDiscovery {

    private static final Logger log = LoggerFactory.getLogger(NodeDiscovery.class);
    // local cache of all vertx cluster nodes.
    private List<String> nodes;

    private final String nodeId;
    private final Vertx vertx;
    private final ConsulClientOptions consulClientOptions;
    private final ConsulClient consulClient;
    private final NodeListener nodeListener;

    public NodeDiscovery(Vertx vertx,
                         ConsulClientOptions options,
                         ConsulClient consulClient,
                         NodeListener nodeListener,
                         String nodeId) {
        this.consulClient = consulClient;
        this.vertx = vertx;
        this.consulClientOptions = options;
        // nodeListener potentially can be null until it is initialized within nodeListener() method of ConsulClusterManager.
        // ALWAYS AND ALWAYS do checking on null while performing anything on nodeListener object.
        this.nodeListener = nodeListener;
        this.nodeId = nodeId;
    }


    /**
     * Discovers nodes that are currently available within the cluster.
     */
    public Future<List<String>> discoverClusterNodes() {
        log.trace("Trying to fetch all the nodes that are available within the consul cluster.");
        Future<List<String>> futureNodes = Future.future();
        consulClient.getKeys(ClusterManagerMaps.VERTX_NODES.getName(), result -> {
            if (result.succeeded()) {
                log.trace("List of fetched nodes is: '{}'", result.result());
                this.nodes = result.result();
                futureNodes.complete(result.result());
            } else {
                futureNodes.fail(result.cause());
            }
        });
        return futureNodes;
    }

    /**
     * Listens for a new nodes within the cluster.
     */
    public Watch listenForNewNodes() {
        // - tricky !!! watchers are always executed  within the event loop context !!!
        // - nodeAdded() call must NEVER be called within event loop context ???!!!.
        return Watch.keyPrefix(ClusterManagerMaps.VERTX_NODES.getName(), vertx, consulClientOptions).setHandler(event -> {
            if (event.succeeded()) {
                vertx.executeBlocking(blockingEvent -> {
                    // TODO: is filtering by this nodeId needed ?
                    event.nextResult().getList().stream().map(KeyValue::getKey).forEach(newNodeId -> {
                        log.trace("New node: '{}' has been discovered within the cluster.", newNodeId);
                        nodes.add(newNodeId);
                        if (nodeListener != null) {
                            log.trace("Adding new node: '{}' to nodeListener.", newNodeId);
                            nodeListener.nodeAdded(nodeId);
                        }
                    });
                    blockingEvent.complete();
                }, result -> {
                });
            } else {
                log.error("Couldn't register watcher for service: '{}'. Details: '{}'", nodeId, event.cause().getMessage());
            }
        });
    }

    public List<String> getNodes() {
        return nodes;
    }
}
