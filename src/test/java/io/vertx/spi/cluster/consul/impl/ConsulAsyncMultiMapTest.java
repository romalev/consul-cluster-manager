package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo;
import io.vertx.core.net.impl.ServerID;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ChoosableIterable;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.spi.cluster.consul.ConsulAgent;
import io.vertx.spi.cluster.consul.impl.cache.CacheManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Testing {@link ConsulAsyncMultiMap}.
 *
 * @author Roman Levytskyi
 */
@RunWith(VertxUnitRunner.class)
public class ConsulAsyncMultiMapTest {

    static {
        System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory");
    }

    private static final String MAP_NAME = "__vertx.subs";

    private static final String nodeA = UUID.randomUUID().toString();
    private static final String nodeB = UUID.randomUUID().toString();
    private static final boolean isEmbeddedConsulAgentEnabled = true;

    private static ConsulAgent consulAgent;
    private static ConsulClient consulClient;
    private static ConsulClientOptions cCOps;
    private static AsyncMultiMap<String, ClusterNodeInfo> consulAsyncMultiMapNodeA;
    private static AsyncMultiMap<String, ClusterNodeInfo> consulAsyncMultiMapNodeB;

    @ClassRule
    public static RunTestOnContext rule = new RunTestOnContext();

    @BeforeClass
    public static void setUp(TestContext context) {
        Async async = context.async();
        rule.vertx().executeBlocking(workerThread -> {
            if (isEmbeddedConsulAgentEnabled) {
                consulAgent = new ConsulAgent();
                consulAgent.start();
                cCOps = new ConsulClientOptions().setPort(consulAgent.getPort());
            } else {
                cCOps = new ConsulClientOptions();
            }
            consulClient = ConsulClient.create(rule.vertx(), cCOps);
            CacheManager.init(rule.vertx(), cCOps);
            workerThread.complete();
        }, res ->
                // creating two maps (this sort simulates the situation with two nodes that are subscribed to event bus)
                consulClient.createSession(sessionIdForNodeA -> {
                    if (sessionIdForNodeA.failed()) context.fail(sessionIdForNodeA.cause());
                    else {
                        consulAsyncMultiMapNodeA = new ConsulAsyncMultiMap<>(MAP_NAME, rule.vertx(), consulClient, sessionIdForNodeA.result(), nodeA);
                        consulClient.createSession(sessionIdForNodeB -> {
                            if (sessionIdForNodeB.failed()) context.fail(sessionIdForNodeB.cause());
                            else {
                                consulAsyncMultiMapNodeB = new ConsulAsyncMultiMap<>(MAP_NAME, rule.vertx(), consulClient, sessionIdForNodeB.result(), nodeB);
                                async.complete();
                            }
                        });
                    }
                }));
    }

    @Test
    public void verify_putAndGet(TestContext context) {
        Async async = context.async();
        // given
        ClusterNodeInfo clusterNodeAInfo = new ClusterNodeInfo(nodeA, new ServerID(8080, "localhost"));
        ClusterNodeInfo clusterNodeBInfo = new ClusterNodeInfo(nodeB, new ServerID(8081, "localhost"));
        String address = "users.putAndGet";

        consulAsyncMultiMapNodeA.add(address, clusterNodeAInfo, cHandler_1 -> {
            context.assertTrue(cHandler_1.succeeded());
            consulAsyncMultiMapNodeB.add(address, clusterNodeBInfo, cHandler_2 -> {
                context.assertTrue(cHandler_2.succeeded());
                /* until this moment consul agent KV store' __vertx.subs key contains:

                 *                                       -> nodeA's UUID -> localhost:8080
                 * __vertx.subs ->  users.create.channel -> nodeB's UUID -> localhost:8081
                 */
                consulAsyncMultiMapNodeA.get(address, resultHandler -> {
                    context.assertTrue(resultHandler.succeeded());
                    Set<ClusterNodeInfo> expectedNodeSet = Stream.of(clusterNodeAInfo, clusterNodeBInfo).collect(Collectors.toSet());
                    ChoosableIterable<ClusterNodeInfo> result = resultHandler.result();
                    result.forEach(clusterNodeInfo -> context.assertTrue(expectedNodeSet.contains(clusterNodeAInfo)));
                    async.complete();
                });

            });
        });
    }

    @Test
    public void verify_remove(TestContext context) {
        Async async = context.async();
        // given
        ClusterNodeInfo clusterNodeAInfo = new ClusterNodeInfo(nodeA, new ServerID(8080, "localhost"));
        ClusterNodeInfo clusterNodeBInfo = new ClusterNodeInfo(nodeB, new ServerID(8081, "localhost"));
        String address = "users.remove";

        consulAsyncMultiMapNodeA.add(address, clusterNodeAInfo, cHandler_1 -> {
            context.assertTrue(cHandler_1.succeeded());
            consulAsyncMultiMapNodeB.add(address, clusterNodeBInfo, cHandler_2 -> {
                context.assertTrue(cHandler_2.succeeded());
                ClusterNodeInfo notExistingClusterNodeInfo = new ClusterNodeInfo(UUID.randomUUID().toString(), new ServerID(1111, "localhost"));
                consulAsyncMultiMapNodeA.remove(address, notExistingClusterNodeInfo, cHandler_3 -> {
                    context.assertTrue(cHandler_3.succeeded());
                    context.assertFalse(cHandler_3.result());
                    consulAsyncMultiMapNodeA.get(address, rHandler_1 -> {
                        context.assertTrue(rHandler_1.succeeded());
                        Set<ClusterNodeInfo> expectedNodeSet = Stream.of(clusterNodeAInfo, clusterNodeBInfo).collect(Collectors.toSet());
                        rHandler_1.result().forEach(clusterNodeInfo -> context.assertTrue(expectedNodeSet.contains(clusterNodeAInfo)));
                        consulAsyncMultiMapNodeA.remove(address, clusterNodeBInfo, cHandler_4 -> {
                            context.assertTrue(cHandler_4.succeeded());
                            context.assertTrue(cHandler_4.result());
                            consulAsyncMultiMapNodeB.get(address, rHandler_2 -> {
                                context.assertTrue(rHandler_2.succeeded());
                                List<ClusterNodeInfo> receivedSet = new ArrayList<>();
                                rHandler_2.result().forEach(clusterNodeInfo -> receivedSet.add(clusterNodeAInfo));
                                context.assertEquals(receivedSet.get(0), clusterNodeAInfo);
                                async.complete();
                            });
                        });
                    });
                });
            });
        });
    }

    @Test
    public void verify_removeAllForValue(TestContext context) {
        Async async = context.async();
        // given
        String users = "users.removeAllForValue";
        ClusterNodeInfo usersNodeASub = new ClusterNodeInfo(nodeA, new ServerID(8080, "192.168.0.1"));
        ClusterNodeInfo usersNodeBSub = new ClusterNodeInfo(nodeB, new ServerID(8081, "192.168.0.2"));

        String posts = "posts.removeAllForValue";
        ClusterNodeInfo postsNodeASub = new ClusterNodeInfo(nodeA, new ServerID(8080, "192.168.0.1"));
        ClusterNodeInfo postsNodeBSub = new ClusterNodeInfo(nodeB, new ServerID(8081, "192.168.0.2"));

        // when
        Future<Void> future_1 = Future.future();
        consulAsyncMultiMapNodeA.add(users, usersNodeASub, future_1.completer());
        Future<Void> future_2 = Future.future();
        consulAsyncMultiMapNodeB.add(users, usersNodeBSub, future_2.completer());
        Future<Void> future_3 = Future.future();
        consulAsyncMultiMapNodeA.add(posts, postsNodeASub, future_3.completer());
        Future<Void> future_4 = Future.future();
        consulAsyncMultiMapNodeB.add(posts, postsNodeBSub, future_4.completer());

        Future<Void> remove_f = Future.future();
        consulAsyncMultiMapNodeA.removeAllForValue(usersNodeASub, remove_f.completer());

        CompositeFuture.all(future_1, future_2, future_3, future_4).compose(handler -> {
            context.assertTrue(handler.succeeded());
            Future<Void> res = Future.future();
            remove_f.setHandler(rHandler -> {
                context.assertTrue(rHandler.succeeded());
                consulAsyncMultiMapNodeA.get(users, usersSubs -> {
                    context.assertTrue(usersSubs.succeeded());
                    // usersNodeASub was hopefully removed.
                    Set<ClusterNodeInfo> expectedNodeSet = Stream.of(usersNodeBSub).collect(Collectors.toSet());
                    usersSubs.result().forEach(clusterNodeInfo -> context.assertTrue(expectedNodeSet.contains(clusterNodeInfo)));
                    consulAsyncMultiMapNodeB.get(posts, postsSubs -> {
                        context.assertTrue(postsSubs.succeeded());
                        Set<ClusterNodeInfo> expectedNodeBSet = Stream.of(postsNodeBSub).collect(Collectors.toSet());
                        postsSubs.result().forEach(clusterNodeInfo -> context.assertTrue(expectedNodeBSet.contains(clusterNodeInfo)));
                        res.complete();
                    });
                });
            });
            return res;
        }).setHandler(event -> async.complete());
    }

    @AfterClass
    public static void tearDown(TestContext context) {
        CacheManager.close();
        rule.vertx().close(context.asyncAssertSuccess());
        if (isEmbeddedConsulAgentEnabled) consulAgent.stop();
    }

}
