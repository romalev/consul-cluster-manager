package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.Watch;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Central Consul node manager.
 *
 * @author Roman Levytskyi
 */
public final class NodeManager {

    private static final Logger log = LoggerFactory.getLogger(NodeManager.class);

    private final String nodeId;
    private final List<String> nodes;
    private final String nodeMapName;
    private final Vertx vertx;
    private final ConsulClientOptions consulClientOptions;
    private final ConsulClient consulClient;
    private final NodeListener nodeListener;

    public NodeManager(Vertx vertx, ConsulClientOptions options, ConsulClient consulClient, NodeListener nodeListener, String nodeId, String nodeMapName) {
        Objects.requireNonNull(vertx);
        Objects.requireNonNull(options);
        Objects.requireNonNull(consulClient);
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(nodeMapName);
        this.consulClient = consulClient;
        this.vertx = vertx;
        this.consulClientOptions = options;
        // nodeListener potentially can be null until it is initialized within nodeListener() method of ConsulClusterManager.
        // ALWAYS AND ALWAYS do checking on null while performing anything on nodeListener object.
        this.nodeListener = nodeListener;
        this.nodeId = nodeId;
        this.nodeMapName = nodeMapName;
        this.nodes = getAvailableClusterNodes();

    }


    /**
     * Returns nodes that are currently available within the cluster.
     */
    private List<String> getAvailableClusterNodes() {
        log.trace("Trying to fetch all the nodes that are available within the consul cluster.");

        CompletableFuture<List<String>> futureNodes = new CompletableFuture<>();
        consulClient.getKeys(nodeMapName, result -> {
            if (result.succeeded()) {
                futureNodes.complete(result.result());
            } else {
                futureNodes.completeExceptionally(result.cause());
            }
        });

        List<String> nodes;
        try {
            nodes = futureNodes.get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Exception has occurred while fetching all the nodes within the cluster due to: '{}'.", e.getCause().toString());
            throw new VertxException(e);
        }

        log.trace("List of fetched nodes is: '{}'", nodes);
        return nodes;
    }

    public List<String> getNodes() {
        return nodes;
    }

    /**
     * Listens for a new nodes within the cluster.
     */
    public Watch listenForNewNodes() {
        // - tricky !!! watchers are always executed  within the event loop context !!!
        // - nodeAdded() call must NEVER be called within event loop context ???!!!.
        return Watch.keyPrefix(nodeMapName, vertx, consulClientOptions).setHandler(event -> {
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
}
