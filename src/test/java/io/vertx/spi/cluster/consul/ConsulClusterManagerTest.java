package io.vertx.spi.cluster.consul;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Central points of all tests for consul based cluster manager.
 *
 * @author Roman Levytskyi
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        ConsulAsyncMapTest.class,
        ConsulClusterManagerOptionsTest.class
})
public class ConsulClusterManagerTest {
}
