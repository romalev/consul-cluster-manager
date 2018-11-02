package io.vertx.core.eventbus;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Verticle;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.spi.cluster.consul.ConsulClusterManager;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.instanceOf;

public class ConsulClusteredEventBusTest extends ClusteredEventBusTest {

  @Override
  protected ClusterManager getClusterManager() {
    return new ConsulClusterManager(new ConsulClientOptions());
  }

  @Test
  public void testSendWhileUnsubscribing() throws Exception {
    startNodes(2);

    AtomicBoolean unregistered = new AtomicBoolean();

    Verticle sender = new AbstractVerticle() {

      @Override
      public void start() throws Exception {
        getVertx().runOnContext(v -> sendMsg());
      }

      private void sendMsg() {
        if (!unregistered.get()) {
          getVertx().eventBus().send("whatever", "marseille");
          vertx.setTimer(1, id -> {
            sendMsg();
          });
        } else {
          getVertx().eventBus().send("whatever", "marseille", ar -> {
            Throwable cause = ar.cause();
            assertThat(cause, instanceOf(ReplyException.class));
            ReplyException replyException = (ReplyException) cause;
            assertEquals(ReplyFailure.NO_HANDLERS, replyException.failureType());
            testComplete();
          });
        }
      }
    };

    Verticle receiver = new AbstractVerticle() {
      boolean unregisterCalled;

      @Override
      public void start(Future<Void> startFuture) throws Exception {
        EventBus eventBus = getVertx().eventBus();
        MessageConsumer<String> consumer = eventBus.consumer("whatever");
        consumer.handler(m -> {
          if (!unregisterCalled) {
            consumer.unregister(v -> unregistered.set(true));
            unregisterCalled = true;
          }
          m.reply("ok");
        }).completionHandler(startFuture);
      }
    };

    CountDownLatch deployLatch = new CountDownLatch(1);
    vertices[0].exceptionHandler(this::fail).deployVerticle(receiver, onSuccess(receiverId -> {
      vertices[1].exceptionHandler(this::fail).deployVerticle(sender, onSuccess(senderId -> {
        deployLatch.countDown();
      }));
    }));
    awaitLatch(deployLatch);

    await();

    CountDownLatch closeLatch = new CountDownLatch(2);
    vertices[0].close(v -> closeLatch.countDown());
    vertices[1].close(v -> closeLatch.countDown());
    awaitLatch(closeLatch);
  }
}
