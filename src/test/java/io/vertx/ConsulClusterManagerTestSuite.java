package io.vertx;

import io.vertx.core.ConsulClusteredHATest;
import io.vertx.core.eventbus.Consul_AP_ClusteredEventBusTest;
import io.vertx.core.eventbus.Consul_CP_ClusteredEventBusTest;
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
  Consul_AP_MultiMapTest.class,
  Consul_CP_MultiMapTest.class,
  ConsulSyncMapTest.class,
  ConsulClusteredAsyncMapTest.class,
  ConsulClusteredAsynchronousLockTest.class,
  ConsulClusteredSharedCounterTest.class,
  Consul_AP_ClusteredEventBusTest.class,
  Consul_CP_ClusteredEventBusTest.class,
  ConsumerRoundRobinTest.class,
  ConsulClusteredHATest.class, // ???
  // TODO: get tests below done!
  // ConsulClusteredComplexHATest.class,
  // ConsulFaultToleranceTest.class,
  // ConsulClusteredSessionHandlerTest.class

})
public class ConsulClusterManagerTestSuite {
}
