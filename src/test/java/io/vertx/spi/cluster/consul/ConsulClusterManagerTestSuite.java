package io.vertx.spi.cluster.consul;

import io.vertx.spi.cluster.consul.impl.ConsulAsyncMapTest;
import io.vertx.spi.cluster.consul.impl.ConsulAsyncMultiMapTest;
import io.vertx.spi.cluster.consul.impl.ConsulSyncMapTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        ConsulSyncMapTest.class,
        ConsulAsyncMultiMapTest.class,
        ConsulAsyncMapTest.class
})
public class ConsulClusterManagerTestSuite {
}
