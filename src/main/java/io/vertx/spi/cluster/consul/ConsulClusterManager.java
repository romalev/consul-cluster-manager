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

import java.util.List;
import java.util.Map;

/**
 * Cluster manager that uses Consul. See README for more details.
 *
 * @author Roman Levytskyi
 */
public class ConsulClusterManager implements ClusterManager {

    private Vertx vertx;

    @Override
    public void setVertx(Vertx vertx) {
        this.vertx = vertx;
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
    public void join(Handler<AsyncResult<Void>> resultHandler) {

    }

    @Override
    public void leave(Handler<AsyncResult<Void>> resultHandler) {

    }

    @Override
    public boolean isActive() {
        return false;
    }
}
