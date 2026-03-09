/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.spark.harness;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Lightweight HTTP server that captures OpenLineage events emitted by the OL Spark listener.
 *
 * <p>OL is configured with HTTP transport ({@code spark.openlineage.transport.type=http}) pointing
 * to this server. Each POST to {@code /api/v1/lineage} is recorded with a capture timestamp for
 * per-event timing. The server starts on a random free port; use {@link #getPort()} to obtain it.
 *
 * <p>Uses JDK's built-in {@code com.sun.net.httpserver.HttpServer} — no extra dependencies.
 *
 * <pre>{@code
 * EventCaptureServer server = new EventCaptureServer();
 * SparkSession spark = buildSparkSession(server.getPort());
 * // ... run harness ...
 * System.out.println("Events captured: " + server.getEvents().size());
 * server.stop();
 * }</pre>
 */
public class EventCaptureServer {

  private final HttpServer server;
  private final List<CapturedEvent> events = new CopyOnWriteArrayList<>();
  private final long startNanoTime = System.nanoTime();

  public EventCaptureServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), /*backlog=*/ 20);
    // OL HTTP transport POSTs to /api/v1/lineage by default
    server.createContext("/api/v1/lineage", this::handleLineagePost);
    // Some OL versions post to root path — capture both
    server.createContext("/", this::handleLineagePost);
    // Use daemon threads so the JVM can exit even if stop() is not called (e.g. on test failure).
    AtomicInteger threadId = new AtomicInteger();
    ThreadFactory daemonFactory = r -> {
      Thread t = new Thread(r, "event-capture-" + threadId.incrementAndGet());
      t.setDaemon(true);
      return t;
    };
    server.setExecutor(Executors.newCachedThreadPool(daemonFactory));
    server.start();
  }

  private void handleLineagePost(HttpExchange exchange) throws IOException {
    try {
      if ("POST".equalsIgnoreCase(exchange.getRequestMethod())) {
        byte[] body = exchange.getRequestBody().readAllBytes();
        long offsetNs = System.nanoTime() - startNanoTime;
        events.add(new CapturedEvent(offsetNs, new String(body, StandardCharsets.UTF_8)));
        exchange.sendResponseHeaders(201, -1);
      } else {
        exchange.sendResponseHeaders(405, -1);
      }
    } finally {
      exchange.close();
    }
  }

  /** The port the server is listening on. Pass to {@code spark.openlineage.transport.url}. */
  public int getPort() {
    return server.getAddress().getPort();
  }

  /** All events captured since the server started, in arrival order. */
  public List<CapturedEvent> getEvents() {
    return Collections.unmodifiableList(events);
  }

  /** Stop the server. Called by {@link HarnessRunner} after the run completes. */
  public void stop() {
    server.stop(0);
  }

  /** One captured OL event with its arrival timestamp. */
  public static class CapturedEvent {
    /** Nanoseconds since the server was created. */
    public final long offsetNs;
    /** Raw JSON body of the POST. */
    public final String body;

    public CapturedEvent(long offsetNs, String body) {
      this.offsetNs = offsetNs;
      this.body = body;
    }

    @Override
    public String toString() {
      return String.format("CapturedEvent{offsetMs=%d, bodyLen=%d}",
          offsetNs / 1_000_000, body.length());
    }
  }
}
