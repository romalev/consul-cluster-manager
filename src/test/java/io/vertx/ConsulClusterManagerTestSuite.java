package io.vertx;

import io.vertx.core.shareddata.ConsulAsyncMultiMapTest;
import io.vertx.core.shareddata.ConsulClusteredAsyncMapTest;
import io.vertx.core.shareddata.ConsulClusteredAsynchronousLockTest;
import io.vertx.core.shareddata.ConsulClusteredSharedCounterTest;
import io.vertx.spi.cluster.consul.impl.ConsulSyncMapTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Central test suite.
 * <p>
 * To enable slf4 logging specify this as VM options:
 *
 *
 * @author Roman Levytskyi
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
  ConsulAsyncMultiMapTest.class,
  ConsulClusteredAsynchronousLockTest.class,
  ConsulClusteredSharedCounterTest.class,
  ConsulClusteredAsyncMapTest.class,
  ConsulSyncMapTest.class
})
public class ConsulClusterManagerTestSuite {
}
