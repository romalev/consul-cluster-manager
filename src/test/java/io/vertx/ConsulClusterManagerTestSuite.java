package io.vertx;

import io.vertx.core.shareddata.ConsulAsyncMultiMapTest;
import io.vertx.core.shareddata.ConsulClusteredAsynchronousLockTest;
import io.vertx.core.shareddata.ConsulClusteredSharedCounterTest;
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
  ConsulAsyncMultiMapTest.class,
  ConsulClusteredAsynchronousLockTest.class,
  ConsulClusteredSharedCounterTest.class
})
public class ConsulClusterManagerTestSuite {
}
