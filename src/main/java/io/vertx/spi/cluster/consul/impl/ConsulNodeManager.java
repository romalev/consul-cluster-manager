package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.ext.consul.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static io.vertx.spi.cluster.consul.impl.ConversationUtils.asConsulEntry;

/**
 * Main manager is accountable for:
 * <p>
 * - Node registration within the cluster. Every new consul service and new entry within __vertx.nodes represent actual vertx node.
 * Don't confuse vertx node with consul (native) node - these are completely different things.
 * Note: every vetx node that joins the cluster IS tagged with NODE_COMMON_TAG = "vertx-clustering".
 * <p>
 * - Node discovery. Nodes discovery for a new node happening while this new node is joining the cluster - it looks up all entry withing __vertx.nodes.
 * <p>
 * - HaInfo pre-initialization. We need to pre-build haInfo map at the "node is joining" stage to be able later on to properly
 * initialize consul sync map. We can't block the event loop thread when it gets the consul sync map. (Consul sync map holds haInfo.)
 * <p>
 * - Creating dummy TCP server to receive and acknowledge heart beats messages from consul.
 * <p>
 * - Creating TCP check on the consul side that sends heart beats messages to previously mentioned tcp server - this allow consul agent to be aware of
 * what is going on within the cluster (node is active if it acknowledges hear beat message, inactive - otherwise.)
 * <p>
 * - Creating consul session. Consul session is used to make consul map entries ephemeral (every entry with that is created with special KV options referencing session id gets automatically deleted
 * from the consul cluster once session gets invalidated).
 * In this cluster manager case session will get invalidated when:
 * <li>health check gets unregistered.</li>
 * <li>health check goes to critical state (when this.netserver doesn't acknowledge the consul's heartbeat message).</li>
 * <li>session is explicitly destroyed.</li>
 *
 * @author Roman Levytskyi
 */
public class ConsulNodeManager extends ConsulMap<String, String> implements ConsulKvListener {

    private static final Logger log = LoggerFactory.getLogger(ConsulNodeManager.class);
    private static final String HA_INFO_MAP = "__vertx.haInfo";
    private static final String TCP_CHECK_INTERVAL = "10s";

    private final CacheManager cM;
    private final String checkId;
    private final String sessionName;
    private final Map haInfoMap = new ConcurrentHashMap<>(); // dedicated cache to initialize and keep haInfo.
    private JsonObject tcpAddress = new JsonObject(); // tcp address of node.
    // local cache of all vertx cluster nodes.
    private Set<String> nodes = new HashSet<>();
    private NetServer netServer;
    // consul session id used to lock map entries.
    private String sessionId;
    private NodeListener nodeListener;

    public ConsulNodeManager(Vertx vertx, ConsulClient cC, CacheManager cM, String nodeId) {
        super("__vertx.nodes", nodeId, vertx, cC);
        this.cM = cM;
        this.checkId = "tcpCheckFor-" + nodeId;
        this.sessionName = "sessionFor-" + nodeId;
        printLocalNodeMap();
    }


    /**
     * Joins node with the cluster in async manner.
     */
    public void join(Handler<AsyncResult<Void>> resultHandler) {
        createTcpServer()
                .compose(aVoid -> registerService())
                .compose(aVoid -> registerTcpCheck())
                .compose(aVoid -> registerSession())
                .compose(aVoid -> registerNode())
                .compose(aVoid -> discoverNodes())
                .compose(aVoid -> initHaInfo())
                .setHandler(resultHandler);
    }

    /**
     * Node leaving the cluster.
     */
    public void leave(Handler<AsyncResult<Void>> resultHandler) {
        haInfoMap.clear();
        netServer.close();
        nodes.clear();
        destroySession()
                .compose(aVoid -> deregisterNode())
                .compose(aVoid -> deregisterTcpCheck())
                .setHandler(resultHandler);
    }

    /**
     * @return available nodes in cluster.
     */
    public List<String> getNodes() {
        return new ArrayList<>(nodes);
    }

    /**
     * @return session id - used to make consul map entries ephemeral.
     */
    public String getSessionId() {
        return sessionId;
    }

    @Override
    public void entryUpdated(EntryEvent event) {
        vertx.executeBlocking(workingThread -> {
            String receivedNodeId = actualKey(event.getEntry().getKey());
            switch (event.getEventType()) {
                case WRITE: {
                    boolean added = nodes.add(receivedNodeId);
                    if (added)
                        log.trace("[" + nodeId + "]" + " New node: " + receivedNodeId + " has joined the cluster.");
                    if (nodeListener != null) {
                        nodeListener.nodeAdded(receivedNodeId);
                        log.trace("[" + nodeId + "]" + " Node: " + receivedNodeId + " has been added to nodeListener.", receivedNodeId);
                    }
                    break;
                }
                case REMOVE: {
                    boolean removed = nodes.remove(receivedNodeId);
                    if (removed) log.trace("[" + nodeId + "]" + " Node: " + receivedNodeId + " has left the cluster.");
                    if (nodeListener != null && removed) {
                        nodeListener.nodeLeft(receivedNodeId);
                        log.trace("[" + nodeId + "]" + " Node: " + receivedNodeId + " has been removed from nodeListener.");
                    }
                    break;
                }
            }
            workingThread.complete();
        }, result -> {
        });

    }

    public void initNodeListener(NodeListener nodeListener) {
        this.nodeListener = nodeListener;
        Watch<KeyValueList> watch = cM.createAndGetMapWatch(name);
        watch.setHandler(kvWatchHandler()).start();
    }


    /**
     * @param <K> represents key type.
     * @param <V> represents value type.
     * @return pre-initialized cache that is later used to build consul sync map.
     */
    public <K, V> Map<K, V> getHaInfo() {
        return haInfoMap;
    }

    /**
     * Initializes haInfo map.
     *
     * @param <K> represents key type.
     * @param <V> represents value type.
     * @return completed future if haInfo is initialized successfully, failed future - otherwise.
     */
    private <K, V> Future<Void> initHaInfo() {
        Future<Void> futureHaInfoCache = Future.future();
        consulClient.getValues(HA_INFO_MAP, futureMap -> {
            if (futureMap.succeeded()) {
                List<KeyValue> keyValueList = getKeyValueListOrEmptyList(futureMap.result());
                keyValueList.forEach(keyValue -> {
                    try {
                        ConsulEntry<K, V> entry = asConsulEntry(keyValue.getValue());
                        haInfoMap.put(entry.getKey(), entry.getValue());
                    } catch (Exception e) {
                        log.error("[" + nodeId + "]" + " - Failed to decode an entry of haInfo.", e);
                        futureHaInfoCache.fail(e);
                    }
                });
                log.trace("[" + nodeId + "] " + HA_INFO_MAP + " has been pre-built: " + Json.encodePrettily(haInfoMap));
                futureHaInfoCache.complete();
            } else {
                log.error("[" + nodeId + "]" + " Failed to pre-build " + HA_INFO_MAP, futureMap.cause());
                futureHaInfoCache.fail(futureMap.cause());
            }
        });
        return futureHaInfoCache;
    }

    /**
     * Gets the vertx node's dedicated service registered within consul agent.
     */
    private Future<Void> registerService() {
        Future<Void> future = Future.future();
        ServiceOptions serviceOptions = new ServiceOptions();
        serviceOptions.setName(nodeId);
        serviceOptions.setAddress(tcpAddress.getString("host"));
        serviceOptions.setPort(tcpAddress.getInteger("port"));
        serviceOptions.setTags(Collections.singletonList("vertx-clustering"));
        serviceOptions.setId(nodeId);

        consulClient.registerService(serviceOptions, asyncResult -> {
            if (asyncResult.failed()) {
                netServer.close();
                log.error("[" + nodeId + "]" + " - Failed to register node's service.", asyncResult.cause());
                future.fail(asyncResult.cause());
            } else future.complete();
        });
        return future;
    }

    /**
     * Registers node within __vertx.nodes map.
     */
    private Future<Void> registerNode() {
        Future<Void> future = Future.future();
        putValue(nodeId, tcpAddress.encode(), new KeyValueOptions().setAcquireSession(sessionId)).setHandler(asyncResult -> {
            if (asyncResult.failed()) {
                netServer.close();
                log.error("[" + nodeId + "]" + " - Failed to put node: " + " to: " + name, asyncResult.cause());
                future.fail(asyncResult.cause());
            } else future.complete();
        });
        return future;
    }

    /**
     * Gets the vertx node de-registered from consul cluster.
     *
     * @return completed future if vertx node has been successfully de-registered from consul cluster, failed future - otherwise.
     */
    private Future<Void> deregisterNode() {
        Future<Void> future = Future.future();
        consulClient.deregisterService(nodeId, event -> {
            if (event.failed()) {
                log.error("[" + nodeId + "]" + " - Failed to unregister node.", event.cause());
                future.fail(event.cause());
            } else future.complete();
        });
        return future;
    }

    /**
     * Gets the node's tcp check registered within consul .
     */
    private Future<Void> registerTcpCheck() {
        Future<Void> future = Future.future();
        CheckOptions checkOptions = new CheckOptions()
                .setName(checkId)
                .setNotes("This check is dedicated to service with id: " + nodeId)
                .setId(checkId)
                .setTcp(tcpAddress.getString("host") + ":" + tcpAddress.getInteger("port"))
                .setServiceId(nodeId)
                .setInterval(TCP_CHECK_INTERVAL)
                .setDeregisterAfter("10s") // it is still going to be 1 minute.
                .setStatus(CheckStatus.PASSING);

        consulClient.registerCheck(checkOptions, result -> {
            if (result.failed()) {
                log.error("[" + nodeId + "]" + " - Failed to register check: " + checkOptions.getId(), result.cause());
                // try to de-register the node from consul cluster
                deregisterNode();
                future.fail(result.cause());
            } else future.complete();
        });
        return future;
    }

    /**
     * Gets the node's tcp check de-registered in consul.
     */
    private Future<Void> deregisterTcpCheck() {
        Future<Void> future = Future.future();
        consulClient.deregisterCheck(checkId, resultHandler -> {
            if (resultHandler.succeeded()) future.complete();
            else {
                log.error("[" + nodeId + "]" + " - Failed to deregister check: " + checkId, resultHandler.cause());
                future.fail(resultHandler.cause());
            }
        });
        return future;
    }

    /**
     * Discovers nodes that are currently available within the cluster.
     *
     * @return completed future if nodes (consul services) have been successfully fetched from consul cluster,
     * failed future - otherwise.
     */
    private Future<Void> discoverNodes() {
        return consulKeys()
                .compose(list -> {
                    if (list == null) return Future.succeededFuture();
                    nodes = list.stream().map(this::actualKey).collect(Collectors.toSet());
                    log.trace("[" + nodeId + "]" + " - Available nodes within the cluster: " + nodes);
                    return Future.succeededFuture();
                });
    }

    /**
     * Creates consul session. Consul session is used (in context of vertx cluster manager) to create ephemeral map entries.
     *
     * @return completed future if consul session (consul services) has been successfully registered in consul cluster,
     * failed future - otherwise.
     */
    private Future<Void> registerSession() {
        Future<Void> future = Future.future();
        SessionOptions sessionOptions = new SessionOptions()
                .setBehavior(SessionBehavior.DELETE)
                .setName(sessionName)
                .setChecks(Arrays.asList(checkId, "serfHealth"));

        consulClient.createSessionWithOptions(sessionOptions, session -> {
            if (session.succeeded()) {
                log.trace("[" + nodeId + "]" + " - Session: " + session.result() + " has been registered.");
                sessionId = session.result();
                future.complete();
            } else {
                log.error("[" + nodeId + "]" + " - Failed to register the session.", session.cause());
                future.fail(session.cause());
            }
        });
        return future;
    }

    /**
     * Destroys node's session in consul.
     */
    private Future<Void> destroySession() {
        Future<Void> future = Future.future();
        consulClient.destroySession(sessionId, resultHandler -> {
            if (resultHandler.succeeded()) {
                log.trace("[" + nodeId + "]" + " - Session: " + sessionId + " has been successfully destroyed.");
                future.complete();
            } else {
                log.error("[" + nodeId + "]" + " - Failed to destroy session: " + sessionId, resultHandler.cause());
                future.fail(resultHandler.cause());
            }
        });
        return future;
    }

    /**
     * Creates simple tcp server used to receive heart beat messages from consul cluster and acknowledge them.
     */
    private Future<Void> createTcpServer() {
        Future<Void> future = Future.future();
        try {
            tcpAddress.put("host", InetAddress.getLocalHost().getHostAddress());
        } catch (UnknownHostException e) {
            log.error(e);
            future.fail(e);
        }
        netServer = vertx.createNetServer(new NetServerOptions(tcpAddress));
        netServer.connectHandler(event -> {
        }); // node's tcp server acknowledges consul's heartbeat message.
        netServer.listen(listenEvent -> {
            if (listenEvent.succeeded()) {
                tcpAddress.put("port", listenEvent.result().actualPort());
                future.complete();
            } else future.fail(listenEvent.cause());
        });
        return future;
    }

    // TODO: remove it.
    private void printLocalNodeMap() {
        vertx.setPeriodic(15000, handler -> log.trace("[" + nodeId + "]" + " - Nodes are: " + Json.encodePrettily(nodes)));
    }
}
