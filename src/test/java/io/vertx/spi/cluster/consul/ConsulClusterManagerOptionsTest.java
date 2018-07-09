package io.vertx.spi.cluster.consul;

import io.vertx.ext.consul.ServiceOptions;
import io.vertx.spi.cluster.consul.impl.ConsulClusterManagerOptions;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

/**
 * Test for {@link io.vertx.spi.cluster.consul.impl.ConsulClusterManagerOptions}
 */
public class ConsulClusterManagerOptionsTest {

    @Test
    public void verifyDefaultConstructor() {
        ConsulClusterManagerOptions options = new ConsulClusterManagerOptions();

        assertCommon(options);
        Assert.assertNotNull(options.getServiceOptions().getId());
        Assert.assertNotNull(options.getServiceOptions().getAddress());
        Assert.assertNotNull(options.getServiceOptions().getName());
        Assert.assertNotNull(options.getServiceOptions().getTags());
        Assert.assertTrue(options.getServiceOptions().getPort() > 0 && options.getServiceOptions().getPort() < 65535);
    }

    @Test
    public void verifyConstructorWithServiceOptions() {
        String uuid = UUID.randomUUID().toString();
        ServiceOptions serviceOptions = new ServiceOptions();
        serviceOptions.setName("TestService");
        serviceOptions.setAddress("localhost");
        serviceOptions.setId(uuid);
        serviceOptions.setPort(1000);

        ConsulClusterManagerOptions options = new ConsulClusterManagerOptions(serviceOptions);

        Assert.assertEquals("TestService[" + uuid + "]", options.getServiceOptions().getName());
        Assert.assertEquals(uuid, options.getServiceOptions().getId());
        Assert.assertEquals(1000, options.getServiceOptions().getPort());
        Assert.assertEquals("localhost", options.getServiceOptions().getAddress());

        assertCommon(options);
    }

    // TODO: test the other constructors as well.

    private void assertCommon(ConsulClusterManagerOptions options) {
        Assert.assertNotNull(options.getClientOptions());
        Assert.assertNotNull(options.getServiceOptions());
        Assert.assertNotNull(options.getNodeId());

        Assert.assertTrue(options.getServiceOptions().getTags().contains(ConsulClusterManagerOptions.getCommonNodeTag()));
    }
}
