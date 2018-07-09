package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.VertxException;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.ext.consul.ServiceOptions;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Consul cluster manager options - way to get the cluster manager initialized. Given options are based on:
 * <ul>
 * <li>
 * {@link io.vertx.ext.consul.Service} ->
 * each node of the cluster within the this cluster manager implementation is represented as a Consul Service (https://www.consul.io/docs/agent/services.html).
 * </li>
 * <li>
 * {@link io.vertx.ext.consul.ConsulClientOptions} ->
 * this cluster manager implementation is fully based on consul client (https://github.com/vert-x3/vertx-consul-client) so given options are used to get the client initialized.
 * <li/>
 * </ul>
 *
 * @author Roman Levytskyi
 */
public class ConsulClusterManagerOptions {

    private static final Logger log = LoggerFactory.getLogger(ConsulClusterManagerOptions.class);

    // this tag gets added to all nodes (services) that are added by this cluster manager.
    private static final String COMMON_NODE_TAG = "vertx-consul-clustering";

    private final ServiceOptions serviceOptions;
    private final ConsulClientOptions clientOptions;
    private final String nodeId;


    public ConsulClusterManagerOptions(ServiceOptions serviceOptions, ConsulClientOptions clientOptions) {
        Objects.requireNonNull(serviceOptions);
        Objects.requireNonNull(clientOptions);
        this.nodeId = UUID.randomUUID().toString();

        serviceOptions.setName(enhanceServiceName(serviceOptions.getName(), nodeId));
        addTag(serviceOptions);

        this.serviceOptions = serviceOptions;
        this.clientOptions = clientOptions;
    }

    public ConsulClusterManagerOptions(ServiceOptions serviceOptions) {
        Objects.requireNonNull(serviceOptions);

        this.nodeId = serviceOptions.getId() == null ? UUID.randomUUID().toString() : serviceOptions.getId();

        serviceOptions.setName(enhanceServiceName(serviceOptions.getName(), nodeId));
        addTag(serviceOptions);

        this.serviceOptions = serviceOptions;
        this.clientOptions = new ConsulClientOptions();
    }

    public ConsulClusterManagerOptions(ConsulClientOptions clientOptions) {
        Objects.requireNonNull(clientOptions);
        this.nodeId = UUID.randomUUID().toString();
        this.serviceOptions = defaultServiceOptions();
        this.clientOptions = clientOptions;
    }


    public ConsulClusterManagerOptions() {
        this.nodeId = UUID.randomUUID().toString();
        this.serviceOptions = defaultServiceOptions();
        this.clientOptions = new ConsulClientOptions();
    }

    public ServiceOptions getServiceOptions() {
        return serviceOptions;
    }

    public ConsulClientOptions getClientOptions() {
        return clientOptions;
    }

    public String getNodeId() {
        return nodeId;
    }

    public static String getCommonNodeTag() {
        return COMMON_NODE_TAG;
    }

    private ServiceOptions defaultServiceOptions() {
        Objects.requireNonNull(nodeId);
        final ServiceOptions serviceOptions = new ServiceOptions();
        try {
            serviceOptions.setAddress(InetAddress.getLocalHost().getHostAddress());
        } catch (UnknownHostException e) {
            log.error("Error occurred while getting the host name. Details: " + e.getCause().toString());
            throw new VertxException(e);
        }
        serviceOptions.setPort(AvailablePortFinder.find(2000, 64000));

        serviceOptions.setId(this.nodeId);
        serviceOptions.setName(this.nodeId);
        serviceOptions.setTags(Arrays.asList(COMMON_NODE_TAG));
        return serviceOptions;
    }

    private void addTag(ServiceOptions options) {
        List<String> currentTags = options.getTags() == null ? new ArrayList<>() : options.getTags();
        List<String> newTags = new ArrayList<>(currentTags);
        newTags.add(COMMON_NODE_TAG);
        options.setTags(newTags);
    }

    private String enhanceServiceName(String serviceName, String nodeId) {
        return serviceName + "[" + nodeId + "]";
    }
}
