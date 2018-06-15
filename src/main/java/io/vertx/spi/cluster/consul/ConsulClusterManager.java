package io.vertx.spi.cluster.consul;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.ext.consul.ServiceOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Cluster manager that uses Consul. See README for more details.
 *
 * @author Roman Levytskyi
 */
public class ConsulClusterManager implements ClusterManager {

    private static final Logger log = LoggerFactory.getLogger(ConsulClusterManager.class);

    private Vertx vertx;
    private ConsulClient consulClient;
    private volatile boolean active;

    private ServiceOptions serviceOptions;
    private ConsulClientOptions consulClientOptions;

    public ConsulClusterManager(ServiceOptions serviceOptions) {
        log.trace("Initializing ConsulClusterManager with serviceOptions: '{}' by using default ConsulClientOptions.", serviceOptions.toJson().encodePrettily());
        this.serviceOptions = serviceOptions;
        consulClientOptions = new ConsulClientOptions();
    }

    public ConsulClusterManager(ServiceOptions serviceOptions, ConsulClientOptions clientOptions) {
        log.trace("Initializing ConsulClusterManager with serviceOptions: '{}'. ConsulClientOptions are: '{}'.",
                serviceOptions.toJson().encodePrettily(),
                clientOptions.toJson().encodePrettily());
        this.serviceOptions = serviceOptions;
        this.consulClientOptions = clientOptions;
    }

    private void init() {
        consulClient = ConsulClient.create(vertx);
    }

    @Override
    public void setVertx(Vertx vertx) {
        this.vertx = vertx;
        init();
    }

    @Override
    public <K, V> void getAsyncMultiMap(String name, Handler<AsyncResult<AsyncMultiMap<K, V>>> asyncResultHandler) {

    }

    @Override
    public <K, V> void getAsyncMap(String name, Handler<AsyncResult<AsyncMap<K, V>>> asyncResultHandler) {

    }

    @Override
    public <K, V> Map<K, V> getSyncMap(String name) {
        return null;
    }

    @Override
    public void getLockWithTimeout(String name, long timeout, Handler<AsyncResult<Lock>> resultHandler) {

    }

    @Override
    public void getCounter(String name, Handler<AsyncResult<Counter>> resultHandler) {

    }

    @Override
    public String getNodeID() {
        return null;
    }

    @Override
    public List<String> getNodes() {
        return null;
    }

    @Override
    public void nodeListener(NodeListener listener) {

    }

    @Override
    public synchronized void join(Handler<AsyncResult<Void>> resultHandler) {
        log.trace("'{}' is trying to join the cluster.", serviceOptions.getId());
        if (active) {
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
            consulClient.deregisterService(serviceOptions.getId(), resultHandler);
        } else {
            log.warn("'{}' is NOT active.", serviceOptions.getId());
        }
    }

    @Override
    public boolean isActive() {
        return active;
    }
}
