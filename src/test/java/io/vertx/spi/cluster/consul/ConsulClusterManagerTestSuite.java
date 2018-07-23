package io.vertx.spi.cluster.consul;

import io.vertx.spi.cluster.consul.suite.VertxNodeJoiningClusterTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        VertxNodeJoiningClusterTest.class
})
public class ConsulClusterManagerTestSuite {
}
