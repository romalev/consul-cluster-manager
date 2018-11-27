//package io.vertx.ext.web.sstore;
//
//import io.vertx.core.http.*;
//import io.vertx.core.json.JsonObject;
//import io.vertx.core.spi.cluster.ClusterManager;
//import io.vertx.ext.web.Router;
//import io.vertx.ext.web.Session;
//import io.vertx.ext.web.handler.CookieHandler;
//import io.vertx.ext.web.handler.SessionHandler;
//import io.vertx.spi.cluster.consul.ConsulClusterManager;
//import io.vertx.test.core.TestUtils;
//import org.junit.Test;
//
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.atomic.AtomicReference;
//
//// FIX ME
//public class ConsulClusteredSessionHandlerTest extends ClusteredSessionHandlerTest {
//
//  @Override
//  protected ClusterManager getClusterManager() {
//    return new ConsulClusterManager();
//  }
//
//  protected HttpServerOptions getHttpServerOptions() {
//    return new HttpServerOptions().setPort(9898).setHost("localhost");
//  }
//
//  protected HttpClientOptions getHttpClientOptions() {
//    return new HttpClientOptions().setDefaultPort(9898);
//  }
//
//  @Test
//  public void testClusteredSession() throws Exception {
//    CountDownLatch serversReady = new CountDownLatch(3);
//
//    Router router1 = Router.router(vertices[0]);
//    router1.route().handler(CookieHandler.create());
//    SessionStore store1 = ClusteredSessionStore.create(vertices[0]);
//    router1.route().handler(SessionHandler.create(store1));
//    HttpServer server1 = vertices[0].createHttpServer(new HttpServerOptions().setPort(8081).setHost("localhost"));
//    server1.requestHandler(router1);
//    server1.listen(onSuccess(s -> serversReady.countDown()));
//    HttpClient client1 = vertices[0].createHttpClient(new HttpClientOptions());
//
//    Router router2 = Router.router(vertices[1]);
//    router2.route().handler(CookieHandler.create());
//    SessionStore store2 = ClusteredSessionStore.create(vertices[1]);
//    router2.route().handler(SessionHandler.create(store2));
//    HttpServer server2 = vertices[1].createHttpServer(new HttpServerOptions().setPort(8082).setHost("localhost"));
//    server2.requestHandler(router2);
//    server2.listen(onSuccess(s -> serversReady.countDown()));
//    HttpClient client2 = vertices[0].createHttpClient(new HttpClientOptions());
//
//    Router router3 = Router.router(vertices[2]);
//    router3.route().handler(CookieHandler.create());
//    SessionStore store3 = ClusteredSessionStore.create(vertices[2]);
//    router3.route().handler(SessionHandler.create(store3));
//    HttpServer server3 = vertices[2].createHttpServer(new HttpServerOptions().setPort(8083).setHost("localhost"));
//    server3.requestHandler(router3);
//    server3.listen(onSuccess(s -> serversReady.countDown()));
//    HttpClient client3 = vertices[0].createHttpClient(new HttpClientOptions());
//
//    awaitLatch(serversReady);
//
//    router1.route().handler(rc -> {
//      Session sess = rc.session();
//      sess.put("foo", "bar");
//      stuffSession(sess);
//      rc.response().end();
//    });
//
//    router2.route().handler(rc -> {
//      Session sess = rc.session();
//      checkSession(sess);
//      assertEquals("bar", sess.get("foo"));
//      sess.put("eek", "wibble");
//      rc.response().end();
//    });
//
//    router3.route().handler(rc -> {
//      Session sess = rc.session();
//      checkSession(sess);
//      assertEquals("bar", sess.get("foo"));
//      assertEquals("wibble", sess.get("eek"));
//      rc.response().end();
//    });
//
//    AtomicReference<String> rSetCookie = new AtomicReference<>();
//    testRequestBuffer(client1, HttpMethod.GET, 8081, "/", null, resp -> {
//      String setCookie = resp.headers().get("set-cookie");
//      rSetCookie.set(setCookie);
//    }, 200, "OK", null);
//    // FIXME - for now we do an artificial sleep because it's possible the session hasn't been stored properly before
//    // the next request hits the server
//    // https://github.com/vert-x3/vertx-web/issues/93
//    Thread.sleep(1000);
//    testRequestBuffer(client2, HttpMethod.GET, 8082, "/", req -> req.putHeader("cookie", rSetCookie.get()), null, 200, "OK", null);
//    Thread.sleep(1000);
//    testRequestBuffer(client3, HttpMethod.GET, 8083, "/", req -> req.putHeader("cookie", rSetCookie.get()), null, 200, "OK", null);
//  }
//
//  private void stuffSession(Session session) {
//    session.put("somelong", 123456L);
//    session.put("someint", 1234);
//    session.put("someshort", (short) 123);
//    session.put("somebyte", (byte) 12);
//    session.put("somedouble", 123.456d);
//    session.put("somefloat", 123.456f);
//    session.put("somechar", 'X');
//    session.put("somebooleantrue", true);
//    session.put("somebooleanfalse", false);
//    session.put("somestring", "wibble");
//    session.put("somebytes", bytes);
//    session.put("somebuffer", buffer);
//    session.put("someclusterserializable", new JsonObject().put("foo", "bar"));
//  }
//
//  private void checkSession(Session session) {
//    assertEquals(123456L, (long) session.get("somelong"));
//    assertEquals(1234, (int) session.get("someint"));
//    assertEquals((short) 123, (short) session.get("someshort"));
//    assertEquals((byte) 12, (byte) session.get("somebyte"));
//    assertEquals(123.456d, (double) session.get("somedouble"), 0);
//    assertEquals(123.456f, (float) session.get("somefloat"), 0);
//    assertEquals('X', (char) session.get("somechar"));
//    assertTrue(session.get("somebooleantrue"));
//    assertFalse(session.get("somebooleanfalse"));
//    assertEquals("wibble", session.get("somestring"));
//    assertTrue(TestUtils.byteArraysEqual(bytes, session.get("somebytes")));
//    assertEquals(buffer, session.get("somebuffer"));
//    JsonObject json = session.get("someclusterserializable");
//    assertNotNull(json);
//    assertEquals("bar", json.getString("foo"));
//  }
//}
