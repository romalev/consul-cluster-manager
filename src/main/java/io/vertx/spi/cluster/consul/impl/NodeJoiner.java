package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.ext.consul.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

/**
 * @author Roman Levytskyi
 */
public class NodeJoiner {

    private static final Logger log = LoggerFactory.getLogger(NodeJoiner.class);
    private static final String TCP_CHECK_INTERVAL = "5s";

    private final Vertx vertx;
    private final ConsulClient consulClient;
    private final String nodeMapName;

    public NodeJoiner(Vertx vertx, ConsulClient consulClient, String nodeMapName) {
        this.vertx = vertx;
        this.consulClient = consulClient;
        this.nodeMapName = nodeMapName;
    }

    public void join(String nodeId, Handler<AsyncResult<String>> resultHandler) {
        getTcpAddress()
                .compose(this::createTcpServer)
                .compose(tcp -> registerTcpCheck(nodeId, tcp))
                .compose(checkId -> registerSession(nodeId, checkId))
                .compose(sessionId -> registerNode(nodeId, sessionId))
                .setHandler(resultHandler);
    }

    /**
     *
     */
    private Future<String> registerTcpCheck(String nodeId, TcpAddress tcpAddress) {
        Future<String> future = Future.future();
        String checkId = "tcpCheck-" + nodeId;
        CheckOptions checkOptions = new CheckOptions();
        checkOptions.setName(checkId);
        checkOptions.setId(checkId);
        checkOptions.setTcp(tcpAddress.getHost() + ":" + tcpAddress.getPort());
        checkOptions.setInterval(TCP_CHECK_INTERVAL);
        checkOptions.setStatus(CheckStatus.PASSING);

        consulClient.registerCheck(checkOptions, result -> {
            if (result.succeeded()) {
                log.trace("Check has been registered : '{}'", checkOptions.getId());
                future.complete(checkOptions.getId());
            } else {
                log.trace("Can't register check: '{}' due to: '{}'", checkOptions.getId(), result.cause().toString());
                future.fail(result.cause());
            }
        });
        return future;
    }

    private Future<String> registerSession(String nodeId, String checkId) {
        Future<String> future = Future.future();
        SessionOptions sessionOptions = new SessionOptions();
        sessionOptions.setBehavior(SessionBehavior.DELETE);
        sessionOptions.setName("session-" + nodeId);

        sessionOptions.setChecks(Arrays.asList(checkId, "serfHealth"));
        consulClient.createSessionWithOptions(sessionOptions, session -> {
            if (session.succeeded()) {
                log.trace("Session : '{}' has been registered.", session.result());
                future.complete(session.result());
            } else {
                log.error("Couldn't register the session due to: {}", session.cause().toString());
                future.fail(session.cause());
            }
        });
        return future;
    }

    /**
     * Gets the node registered within the Consul cluster.
     *
     * @param sessionId holds the session id
     * @param nodeId    represents the id of the node that joined the cluster.
     * @return future with session id in case of success, otherwise future with message indicating the failure.
     */
    private Future<String> registerNode(String nodeId, String sessionId) {
        Future<String> futureWithSessionId = Future.future();
        consulClient.putValueWithOptions(nodeMapName + "/" + nodeId, nodeId, new KeyValueOptions().setAcquireSession(sessionId), resultHandler -> {
            if (resultHandler.succeeded()) {
                log.trace("Node: '{}' has been registered in consul cluster.");
                futureWithSessionId.complete(sessionId);
            } else {
                log.error("Couldn't register the node: '{}' in consul cluster due to: '{}'", nodeId, futureWithSessionId.cause().toString());
                futureWithSessionId.fail(resultHandler.cause());
            }
        });
        return futureWithSessionId;
    }

    /**
     * Creates simple tcp server used to receive heart beat messages from consul cluster.
     *
     * @param tcpAddress represents host and port of tcp server.
     * @return in case tcp server is created and it listens for heart beat messages -> future with tcp address, otherwise -> future with message
     * indicating the cause of the failure.
     */
    private Future<TcpAddress> createTcpServer(final TcpAddress tcpAddress) {
        Future<TcpAddress> future = Future.future();
        NetServer netServer = vertx.createNetServer(new NetServerOptions().setHost(tcpAddress.getHost()).setPort(tcpAddress.getPort()));
        netServer.connectHandler(event -> log.trace("Health heart beat message sent back to Consul"));
        netServer.listen(listenEvent -> {
            if (listenEvent.succeeded()) future.complete(tcpAddress);
            else future.fail(listenEvent.cause());
        });
        return future;
    }

    /**
     * @return
     */
    private Future<TcpAddress> getTcpAddress() {
        Future<TcpAddress> futureTcp = Future.future();
        try {
            int port = AvailablePortFinder.find(2000, 64000);
            futureTcp.complete(new TcpAddress(InetAddress.getLocalHost().getHostAddress(), port));
        } catch (UnknownHostException e) {
            log.error("Can't get the host address: '{}'", e.getCause().toString());
            futureTcp.fail(e);
        }
        return futureTcp;
    }

    /**
     * Simple representation of tcp address.
     */
    private final class TcpAddress {
        private final String host;
        private final int port;

        public TcpAddress(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        @Override
        public String toString() {
            return "TcpAddress{" +
                    "host='" + host + '\'' +
                    ", port=" + port +
                    '}';
        }
    }
}
