package io.vertx;

import io.vertx.core.ConsulClusteredComplexHATest;
import io.vertx.core.ConsulClusteredHATest;
import io.vertx.core.eventbus.ConsulApClusteredEventBusTest;
import io.vertx.core.eventbus.ConsulCpClusteredEventBusTest;
import io.vertx.core.shareddata.*;
import io.vertx.ext.web.sstore.ConsulClusteredSessionHandlerTest;
import io.vertx.spi.cluster.consul.impl.ConsulNodeWithDefaultHostNameTest;
import io.vertx.spi.cluster.consul.impl.ConsulNodeWithSpecifiedHostNameTest;
import io.vertx.spi.cluster.consul.impl.ConsulSyncMapTest;
import io.vertx.spi.cluster.consul.impl.ConsumerRoundRobinTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Central test suite.
 * <p>
 * To enable slf4 logging specify this as VM options:
 * -ea -Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.SLF4JLogDelegateFactory
 * <p>
 * To run a particular test:
 * ./gradlew test --info --tests io.vertx.core.ConsulClusteredComplexHATest
 * To run all tests:
 * ./gradlew test --info
 * <p>
 * Notes:
 * - FaultToleranceTest is not implemented.
 *
 * @author <a href="mailto:roman.levytskyi.oss@gmail.com">Roman Levytskyi</a>
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
  // HA
  ConsulClusteredHATest.class,
  ConsulClusteredComplexHATest.class,
  // SHARED DATA
  ConsulAsyncApMultiMapTest.class,
  ConsulAsyncCpMultiMapTest.class,
  ConsulClusteredAsyncMultiMapTest.class,
  ConsulClusteredAsyncMapTest.class,
  ConsulClusteredAsynchronousLockTest.class,
  ConsulClusteredSharedCounterTest.class,
  // SYNC MAP
  ConsulSyncMapTest.class,
  // ROUND ROBIN
  ConsumerRoundRobinTest.class,
  // EVENT BUS
  ConsulCpClusteredEventBusTest.class,
  ConsulApClusteredEventBusTest.class,
  ConsulClusteredSessionHandlerTest.class,
  // HOST
  ConsulNodeWithDefaultHostNameTest.class,
  ConsulNodeWithSpecifiedHostNameTest.class
})
public class ConsulClusterManagerTestSuite {
}
