package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Vertx;
import io.vertx.core.shareddata.AsyncMap;
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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Testing {@link ConsulAsyncMap}.
 *
 * @author Roman Levytskyi
 */
@RunWith(VertxUnitRunner.class)
public class ConsulAsyncMapTest {

    static {
        System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory");
    }

    private static final String MAP_NAME = "vertx-test.async";

    private static ConsulAgent consulAgent;
    private static ConsulClient consulClient;
    private static ConsulClientOptions cCOps;
    private static AsyncMap<String, String> consulAsyncMap;

    private static final boolean isEmbeddedConsulAgentEnabled = true;

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
            consulAsyncMap = new ConsulAsyncMap<>(MAP_NAME, Vertx.vertx(), consulClient);
            workerThread.complete();
        }, res -> {
            if (res.succeeded()) {
                async.complete();
            } else {
                context.fail(res.cause());
            }
        });
    }

    @Test
    public void verify_put(TestContext context) {
        // given
        Async async = context.async();
        String key = "keyA";
        String value = "valueA";

        consulAsyncMap.put(key, value, completionHandler -> {
            if (completionHandler.failed()) context.fail(completionHandler.cause());
            else {
                consulAsyncMap.get(key, resultHandler -> {
                    context.assertTrue(resultHandler.succeeded());
                    context.assertEquals(value, resultHandler.result());
                    async.complete();
                });
            }
        });
    }


    /**
     * Verifies the scenario where an entry is being added directly over consul client to consul KV store meaning that given entry is not going to
     * get cached immediately - instead it will be poll down by local consul watch and then it will get cached.
     */
    @Test
    public void verify_putOverConsulClient(TestContext context) {
        Async async = context.async();
        String key = "keyADirect";
        String value = "valueADirect";
        String encodedValue = "";
        try {
            encodedValue = ClusterSerializationUtils.encode(value);
        } catch (IOException e) {
            context.fail(e);
        }

        consulClient.putValue(MAP_NAME + "/" + key, encodedValue, pHandler -> {
            context.assertTrue(pHandler.succeeded());
            consulAsyncMap.get(key, rHandler -> {
                context.assertTrue(rHandler.succeeded());
                context.assertEquals(value, rHandler.result());
                async.complete();
            });
        });
    }

    @Test
    public void verify_putWithTtl(TestContext context) {
        Async async = context.async();
        String key = "keyAWithTtl";
        String value = "valueAWithTtl";

        consulAsyncMap.put(key, value, 10000L, cHandler_1 -> {
            context.assertTrue(cHandler_1.succeeded());
            // let's try to update this entry - it should fail
            consulAsyncMap.put(key, value + "_1", 10000L, cHandler_2 -> context.assertTrue(cHandler_2.failed()));
            rule.vertx().executeBlocking(workerThread -> {
                // why 20000 see https://github.com/hashicorp/consul/issues/1172.
                sleep(20000L, context);
                workerThread.complete();
            }, event ->
                    // an entry should have get invalidated until this moment.
                    consulAsyncMap.get(key, resultHandler -> {
                        context.assertTrue(resultHandler.succeeded());
                        context.assertNull(resultHandler.result());
                        async.complete();
                    }));
        });
    }

    @Test
    public void verify_putWithNullKey(TestContext context) {
        Async async = context.async();
        consulAsyncMap.put(null, "value", cHandler -> {
            context.assertTrue(cHandler.failed());
            context.assertEquals("Key can not be null.", cHandler.cause().getMessage());
            async.complete();
        });
    }

    @Test
    public void verify_putWithNullValue(TestContext context) {
        Async async = context.async();
        consulAsyncMap.put("key", null, cHandler -> {
            context.assertTrue(cHandler.failed());
            context.assertEquals("Value can not be null.", cHandler.cause().getMessage());
            async.complete();
        });
    }

    /**
     * Reminder: consul async map puts the entry only if there is no entry with the key already present.
     * If key already present then the existing value will be returned to the handler, otherwise null.
     */
    @Test
    public void verify_putIfAbsent(TestContext context) {
        Async async = context.async();
        String key = "keyAIfAbsent";
        String value = "valueAIfAbsent";

        consulAsyncMap.putIfAbsent(key, value, cHandler_1 -> {
            context.assertTrue(cHandler_1.succeeded());
            // key wasn't present so null should be returned.
            context.assertNull(cHandler_1.result());
            // now since key is already present then the existing value should be returned.
            consulAsyncMap.putIfAbsent(key, value + "_modified", cHandler_2 -> {
                context.assertTrue(cHandler_2.succeeded());
                context.assertEquals(value, cHandler_2.result());
                async.complete();
            });
        });
    }

    @Test
    public void verify_putIfAbsentWithTtl(TestContext context) {
        Async async = context.async();
        String key = "keyAIfAbsentWithTtl";
        String value = "valueAIfAbsentWithTtl";

        consulAsyncMap.putIfAbsent(key, value, 10000L, cHandler_1 -> {
            context.assertTrue(cHandler_1.succeeded());
            // key wasn't present so null should be returned.
            context.assertNull(cHandler_1.result());
            consulAsyncMap.putIfAbsent(key, value + "_modified", cHandler_2 -> {
                context.assertTrue(cHandler_2.succeeded());
                context.assertEquals(value, cHandler_2.result());
                rule.vertx().executeBlocking(workerThread -> {
                    // why 20000 see https://github.com/hashicorp/consul/issues/1172.
                    sleep(20000L, context);
                    workerThread.complete();
                }, res -> {
                    consulAsyncMap.get(key, rHandler -> {
                        context.assertTrue(rHandler.succeeded());
                        context.assertNull(rHandler.result());
                        async.complete();

                    });
                });
            });
        });
    }

    @Test
    public void verify_remove(TestContext context) {
        Async async = context.async();
        String key = "keyAForRemoval";
        String value = "valueAForRemoval";

        consulAsyncMap.put(key, value, cHandler -> {
            context.assertTrue(cHandler.succeeded());
            consulAsyncMap.remove(key, rHandler_1 -> {
                context.assertTrue(rHandler_1.succeeded());
                context.assertEquals(value, rHandler_1.result());
                consulAsyncMap.get(key, rHandler_2 -> {
                    if (rHandler_2.failed()) context.fail(cHandler.cause());
                    else {
                        context.assertTrue(rHandler_2.succeeded());
                        context.assertNull(rHandler_2.result());
                        async.complete();
                    }
                });
            });
        });
    }

    // verifies whether remove a value from the map happens, only if entry already exists with same value.
    @Test
    public void verify_removeIfPresent(TestContext context) {
        Async async = context.async();
        String key = "keyAForRemovalIfPresent";
        String value = "valueAForRemovalIfPresent";

        consulAsyncMap.removeIfPresent(key, value, bResult_1 -> {
            context.assertTrue(bResult_1.succeeded());
            context.assertFalse(bResult_1.result());
            consulAsyncMap.put(key, value, cHandler -> {
                context.assertTrue(cHandler.succeeded());
                consulAsyncMap.removeIfPresent(key, value, bResult_2 -> {
                    context.assertTrue(bResult_2.succeeded());
                    context.assertTrue(bResult_2.result());
                    consulAsyncMap.get(key, rHandler -> {
                        context.assertTrue(rHandler.succeeded());
                        context.assertNull(rHandler.result());
                        async.complete();
                    });
                });
            });
        });
    }

    // verify : replace the entry happens only if it is currently mapped to some value
    @Test
    public void verify_replace(TestContext context) {
        Async async = context.async();

        String key = "keyAReplace";
        String curValue = "valueAReplaceOldValue";
        String newValue = "valueAReplaceNewValue";
        // keyAReplace doesn't exist yet
        consulAsyncMap.replace(key, newValue, rHandler_1 -> {
            context.assertTrue(rHandler_1.succeeded());
            context.assertNull(rHandler_1.result());
            // now let's put they key
            consulAsyncMap.put(key, curValue, cHandler -> {
                context.assertTrue(cHandler.succeeded());
                // try to replace - should succeeded since key exists.
                consulAsyncMap.replace(key, newValue, rHandler_2 -> {
                    context.assertTrue(rHandler_2.succeeded());
                    context.assertEquals(curValue, rHandler_2.result());
                    consulAsyncMap.get(key, rHandler_3 -> {
                        context.assertTrue(rHandler_3.succeeded());
                        context.assertEquals(newValue, rHandler_3.result());
                        async.complete();
                    });
                });
            });
        });
    }

    @Test
    // verify: replace the entry only if it is currently mapped to a specific value
    public void verify_replaceIfPresent(TestContext context) {
        Async async = context.async();

        String key = "keyAReplaceIfPresent";
        String oldValue = "valueAReplaceIfPresentOld";
        String newValue = "valueAReplaceIfPresentNew";

        // 1. An entry with key doesn't exist
        consulAsyncMap.replaceIfPresent(key, oldValue, newValue, bResult_1 -> {
            context.assertTrue(bResult_1.succeeded());
            context.assertFalse(bResult_1.result());
            // let's put an entry with key and oldValue
            consulAsyncMap.put(key, oldValue, cHandler_1 -> {
                context.assertTrue(cHandler_1.succeeded());
                // 2. an entry doesn't contain old value -> an entry SHOULD NOT BE REPLACED.
                consulAsyncMap.replaceIfPresent(key, oldValue + "_modified", newValue, bResult_2 -> {
                    context.assertTrue(bResult_2.succeeded());
                    context.assertFalse(bResult_2.result());
                    // 3. an entry should get replaced because it is currenlty mapped to oldValue.
                    consulAsyncMap.replaceIfPresent(key, oldValue, newValue, bResult_3 -> {
                        context.assertTrue(bResult_3.succeeded());
                        context.assertTrue(bResult_3.result());
                        // let's eventually verify whether value has been replaced.
                        consulAsyncMap.get(key, rHandler -> {
                            context.assertTrue(rHandler.succeeded());
                            context.assertEquals(newValue, rHandler.result());
                            async.complete();
                        });
                    });
                });
            });

        });
    }

    @Test
    public void verify_entriesAndKeysAndValues(TestContext context) {
        Async async = context.async();

        rule.vertx().executeBlocking(workerThread -> {
            CountDownLatch countDownLatch = new CountDownLatch(3);
            consulAsyncMap.clear(rHandler -> {
                context.assertTrue(rHandler.succeeded());
                consulAsyncMap.put("1", "v1", cHandler -> {
                    context.assertTrue(cHandler.succeeded());
                    countDownLatch.countDown();
                });
                consulAsyncMap.put("2", "v2", cHandler -> {
                    context.assertTrue(cHandler.succeeded());
                    countDownLatch.countDown();
                });
                consulAsyncMap.put("3", "v3", cHandler -> {
                    context.assertTrue(cHandler.succeeded());
                    countDownLatch.countDown();
                });
            });
            try {
                countDownLatch.await();
                workerThread.complete();
            } catch (InterruptedException e) {
                context.fail(e);
            }
        }, res ->
                consulAsyncMap.keys(rKeys -> {
                    context.assertTrue(rKeys.succeeded());
                    Set<String> expectedKeySet = Stream.of("1", "2", "3").collect(Collectors.toSet());
                    context.assertEquals(expectedKeySet, rKeys.result());

                    // values
                    consulAsyncMap.values(rValues -> {
                        context.assertTrue(rValues.succeeded());
                        List<String> expectedValueSet = Stream.of("v1", "v2", "v3").collect(Collectors.toList());
                        context.assertEquals(expectedValueSet, rValues.result());
                        // entries
                        consulAsyncMap.entries(rEntries -> {
                            context.assertTrue(rEntries.succeeded());
                            Map<String, String> expectedEntries = new HashMap<>();
                            expectedEntries.put("1", "v1");
                            expectedEntries.put("2", "v2");
                            expectedEntries.put("3", "v3");
                            context.assertEquals(expectedEntries, rEntries.result());
                            async.complete();
                        });
                    });
                }));
    }

    @AfterClass
    public static void tearDown(TestContext context) {
        CacheManager.close();
        rule.vertx().close(context.asyncAssertSuccess());
        if (isEmbeddedConsulAgentEnabled) consulAgent.stop();
    }

    private void sleep(Long sleepTime, TestContext context) {
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            context.fail(e);
        }
    }
}
