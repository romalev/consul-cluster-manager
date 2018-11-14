package io.vertx;

import io.vertx.core.eventbus.ConsulCpClusteredEventBusTest;
import io.vertx.core.shareddata.*;
import io.vertx.spi.cluster.consul.impl.ConsulSyncMapTest;
import io.vertx.spi.cluster.consul.impl.ConsumerRoundRobinTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Central test suite.
 * <p>
 * To enable slf4 logging specify this as VM options:
 * -ea -Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.SLF4JLogDelegateFactory
 *
 * @author Roman Levytskyi
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
  ConsulApMultiMapTest.class,
  ConsulCpMultiMapTest.class,
  ConsulSyncMapTest.class,
  ConsulClusteredAsyncMapTest.class,
  ConsulClusteredAsynchronousLockTest.class,
  ConsulClusteredSharedCounterTest.class,
  ConsumerRoundRobinTest.class,
  ConsulCpClusteredEventBusTest.class,
  // TODO: get tests below done!
  // ConsulApClusteredEventBusTest.class,
  // ConsulClusteredHATest.class,
  // ConsulClusteredComplexHATest.class,
  // ConsulFaultToleranceTest.class,
  // ConsulClusteredSessionHandlerTest.class

})
public class ConsulClusterManagerTestSuite {
}
