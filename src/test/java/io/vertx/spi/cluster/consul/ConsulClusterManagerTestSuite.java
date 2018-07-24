package io.vertx.spi.cluster.consul;

import io.vertx.spi.cluster.consul.suite.ConsulAsyncMapTest;
import io.vertx.spi.cluster.consul.suite.ConsulAsyncMultiMapTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        ConsulAsyncMapTest.class,
        ConsulAsyncMultiMapTest.class
})
public class ConsulClusterManagerTestSuite {
}
