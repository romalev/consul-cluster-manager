//package io.vertx.core.eventbus;
//
//import io.vertx.core.AbstractVerticle;
//import io.vertx.core.Future;
//import io.vertx.core.Verticle;
//import io.vertx.core.logging.Logger;
//import io.vertx.core.logging.LoggerFactory;
//import io.vertx.core.spi.cluster.ClusterManager;
//import io.vertx.ext.consul.ConsulClientOptions;
//import io.vertx.spi.cluster.consul.ConsulClusterManager;
//import org.junit.Test;
//
//import java.util.ArrayList;
//import java.util.concurrent.ConcurrentLinkedDeque;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.atomic.AtomicBoolean;
//
//import static org.hamcrest.CoreMatchers.instanceOf;
//
//// FIXME
//public class ConsulClusteredEventBusTest extends ClusteredEventBusTest {
//
//    private static final Logger log = LoggerFactory.getLogger(ConsulClusteredEventBusTest.class);
//
//    @Test
//    public void sendNoContext() throws Exception {
//        int size = 1000;
//        ConcurrentLinkedDeque<Integer> expected = new ConcurrentLinkedDeque<>();
//        ConcurrentLinkedDeque<Integer> obtained = new ConcurrentLinkedDeque<>();
//        startNodes(2);
//        CountDownLatch latch = new CountDownLatch(1);
//        vertices[1].eventBus().<Integer>consumer(ADDRESS1, msg -> {
//            obtained.add(msg.body());
//            if (obtained.size() == expected.size()) {
//                assertEquals(new ArrayList<>(expected), new ArrayList<>(obtained));
//                testComplete();
//            }
//        }).completionHandler(ar -> {
//            assertTrue(ar.succeeded());
//            latch.countDown();
//        });
//        latch.await();
//
//        // FIXME
//        Thread.sleep(1000);
//
//        EventBus bus = vertices[0].eventBus();
//        for (int i = 0; i < size; i++) {
//            expected.add(i);
//            bus.send(ADDRESS1, i);
//        }
//        await();
//    }
//
//    @Test
//    public void testSendWhileUnsubscribing() throws Exception {
//        startNodes(2);
//
//        AtomicBoolean unregistered = new AtomicBoolean();
//
//        Verticle sender = new AbstractVerticle() {
//
//            @Override
//            public void start() throws Exception {
//                getVertx().runOnContext(v -> sendMsg());
//            }
//
//            private void sendMsg() {
//                if (!unregistered.get()) {
//                    getVertx().eventBus().send("whatever", "marseille");
//                    vertx.setTimer(1, id -> {
//                        sendMsg();
//                    });
//                } else {
//                    getVertx().eventBus().send("whatever", "marseille", ar -> {
//                        Throwable cause = ar.cause();
//                        assertThat(cause, instanceOf(ReplyException.class));
//                        ReplyException replyException = (ReplyException) cause;
//                        assertEquals(ReplyFailure.NO_HANDLERS, replyException.failureType());
//                        testComplete();
//                    });
//                }
//            }
//        };
//
//        Verticle receiver = new AbstractVerticle() {
//            boolean unregisterCalled;
//
//            @Override
//            public void start(Future<Void> startFuture) throws Exception {
//                EventBus eventBus = getVertx().eventBus();
//                MessageConsumer<String> consumer = eventBus.consumer("whatever");
//                consumer.handler(m -> {
//                    if (!unregisterCalled) {
//                        consumer.unregister(v -> unregistered.set(true));
//                        unregisterCalled = true;
//                    }
//                    m.reply("ok");
//                }).completionHandler(startFuture);
//            }
//        };
//
//        CountDownLatch deployLatch = new CountDownLatch(1);
//        vertices[0].exceptionHandler(this::fail).deployVerticle(receiver, onSuccess(receiverId -> {
//            vertices[1].exceptionHandler(this::fail).deployVerticle(sender, onSuccess(senderId -> {
//                deployLatch.countDown();
//            }));
//        }));
//        awaitLatch(deployLatch);
//
//        await();
//
//        CountDownLatch closeLatch = new CountDownLatch(2);
//        vertices[0].close(v -> closeLatch.countDown());
//        vertices[1].close(v -> closeLatch.countDown());
//        awaitLatch(closeLatch);
//    }
//
//    @Override
//    protected ClusterManager getClusterManager() {
//        return new ConsulClusterManager(new ConsulClientOptions());
//    }
//}