//package io.vertx.core.eventbus;
//
//import io.vertx.core.*;
//import io.vertx.core.spi.cluster.ClusterManager;
//import io.vertx.ext.consul.ConsulClientOptions;
//import io.vertx.spi.cluster.consul.ConsulCluster;
//import io.vertx.spi.cluster.consul.ConsulClusterManager;
//import org.junit.AfterClass;
//import org.junit.BeforeClass;
//import org.junit.Test;
//
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.atomic.AtomicBoolean;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.function.Consumer;
//
//import static org.hamcrest.CoreMatchers.instanceOf;
//
//public class ConsulApClusteredEventBusTest extends ClusteredEventBusTest {
//
//  private static int port;
//
//  @BeforeClass
//  public static void startConsulCluster() {
//    port = ConsulCluster.init();
//  }
//
//  @AfterClass
//  public static void shutDownConsulCluster() {
//    ConsulCluster.shutDown();
//  }
//
//  @Override
//  protected ClusterManager getClusterManager() {
//    ConsulClientOptions options = new ConsulClientOptions()
//      .setPort(port)
//      .setHost("localhost");
//    return new ConsulClusterManager(options, false);
//  }
//
//  /**
//   * This test is a bit enhanced - contains 1.5 sec delays to let local cache catches up with consul KV.
//   * Otherwise assertEquals(ReplyFailure.NO_HANDLERS, replyException.failureType()) will fail since
//   * Handler will be found and TIMEOUT will take place.
//   */
//  @Test
//  public void testSendWhileUnsubscribing() throws Exception {
//    startNodes(2);
//
//    AtomicBoolean unregistered = new AtomicBoolean();
//
//    Verticle sender = new AbstractVerticle() {
//
//      @Override
//      public void start() throws Exception {
//        getVertx().runOnContext(v -> sendMsg());
//      }
//
//      private void sendMsg() {
//        if (!unregistered.get()) {
//          getVertx().eventBus().send("whatever", "marseille");
//          vertx.setTimer(1, id -> {
//            sendMsg();
//          });
//        } else {
//          try {
//            Thread.sleep(1500);
//          } catch (InterruptedException e) {
//            fail(e);
//          }
//          getVertx().eventBus().send("whatever", "marseille", ar -> {
//            Throwable cause = ar.cause();
//            assertThat(cause, instanceOf(ReplyException.class));
//            ReplyException replyException = (ReplyException) cause;
//            assertEquals(ReplyFailure.NO_HANDLERS, replyException.failureType());
//            testComplete();
//          });
//        }
//      }
//    };
//
//    Verticle receiver = new AbstractVerticle() {
//      boolean unregisterCalled;
//
//      @Override
//      public void start(Future<Void> startFuture) throws Exception {
//        EventBus eventBus = getVertx().eventBus();
//        MessageConsumer<String> consumer = eventBus.consumer("whatever");
//        consumer.handler(m -> {
//          if (!unregisterCalled) {
//            consumer.unregister(v -> unregistered.set(true));
//            unregisterCalled = true;
//          }
//          try {
//            Thread.sleep(1500);
//          } catch (InterruptedException e) {
//            fail(e);
//          }
//          m.reply("ok");
//        }).completionHandler(startFuture);
//      }
//    };
//
//    CountDownLatch deployLatch = new CountDownLatch(1);
//    vertices[0].exceptionHandler(this::fail).deployVerticle(receiver, onSuccess(receiverId -> {
//      vertices[1].exceptionHandler(this::fail).deployVerticle(sender, onSuccess(senderId -> {
//        deployLatch.countDown();
//      }));
//    }));
//    awaitLatch(deployLatch);
//
//    await();
//
//    CountDownLatch closeLatch = new CountDownLatch(2);
//    vertices[0].close(v -> closeLatch.countDown());
//    vertices[1].close(v -> closeLatch.countDown());
//    awaitLatch(closeLatch);
//  }
//
//  @Override
//  protected <T> void testPublish(T val, Consumer<T> consumer) {
//    int numNodes = 3;
//    startNodes(numNodes);
//    AtomicInteger count = new AtomicInteger();
//    class MyHandler implements Handler<Message<T>> {
//      @Override
//      public void handle(Message<T> msg) {
//        try {
//          Thread.sleep(1500);
//        } catch (InterruptedException e) {
//          fail(e);
//        }
//        if (consumer == null) {
//          assertFalse(msg.isSend());
//          assertEquals(val, msg.body());
//        } else {
//          consumer.accept(msg.body());
//        }
//        if (count.incrementAndGet() == numNodes - 1) {
//          testComplete();
//        }
//      }
//    }
//    AtomicInteger registerCount = new AtomicInteger(0);
//    class MyRegisterHandler implements Handler<AsyncResult<Void>> {
//      @Override
//      public void handle(AsyncResult<Void> ar) {
//        assertTrue(ar.succeeded());
//        try {
//          Thread.sleep(1500);
//        } catch (InterruptedException e) {
//          fail(e);
//        }
//        if (registerCount.incrementAndGet() == 2) {
//          vertices[0].eventBus().publish(ADDRESS1, val);
//        }
//      }
//    }
//    MessageConsumer reg = vertices[2].eventBus().<T>consumer(ADDRESS1).handler(new MyHandler());
//    reg.completionHandler(new MyRegisterHandler());
//    reg = vertices[1].eventBus().<T>consumer(ADDRESS1).handler(new MyHandler());
//    reg.completionHandler(new MyRegisterHandler());
//    vertices[0].eventBus().publish(ADDRESS1, val);
//    await();
//  }
//
//}
