/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.resilience;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.arpabet.value.MapValue;
import com.arpabet.value.Value;
import com.arpabet.valuerpc.CallOptions;
import com.arpabet.valuerpc.ClientInterceptor;
import com.arpabet.valuerpc.Code;
import com.arpabet.valuerpc.Dialect;
import com.arpabet.valuerpc.FakeServer;
import com.arpabet.valuerpc.VrpcException;
import com.arpabet.valuerpc.client.VrpcClient;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The policies in a client, over a connection: the tests and the examples of
 * the Go implementation ({@code integration_test.go}, {@code example_test.go}),
 * and what a client adds to them here.
 */
@Timeout(30)
class IntegrationTest {

    private static final Dialect D = Dialect.standard();

    /** A function that fails as a server that is not up yet, until it was called often enough. */
    private static AtomicInteger flaky(FakeServer server, String name, int failures, String result) {
        final AtomicInteger calls = new AtomicInteger();
        server.function(name, args -> {
            if (calls.incrementAndGet() <= failures) {
                throw new VrpcException(Code.UNAVAILABLE, "transient");
            }
            return Value.utf8(result);
        });
        return calls;
    }

    /**
     * A retry among the interceptors of a client makes up for a failure that
     * passes, over a connection, without the caller knowing.
     */
    // TestRetryEndToEnd
    @Test
    void retryEndToEnd() throws Exception {
        try (FakeServer server = new FakeServer("mem://resilience-e2e")) {
            final AtomicInteger calls = flaky(server, "flaky", 2, "ok");
            try (VrpcClient client = VrpcClient.builder().address(server.address())
                    .interceptors(Resilience.retry().withMaxAttempts(5).withBackoffMillis(1, 5))
                    .build()) {
                client.connect();
                assertEquals(Value.utf8("ok"), client.callFunction("flaky", null));
                assertEquals(3, calls.get(), "two failures that passed and one success");
            }
        }
    }

    /** The chain, outermost first: circuit breaker, retry, a timeout for each attempt, the call. */
    // Example
    @Test
    void theChainOfTheExample() throws Exception {
        try (FakeServer server = new FakeServer("mem://resilience-example")) {
            final AtomicInteger attempts = flaky(server, "getQuote", 2, "hello");
            try (VrpcClient client = VrpcClient.builder().address(server.address())
                    .interceptors(
                            Resilience.circuitBreaker(),
                            Resilience.retry().withMaxAttempts(5).withBackoffMillis(1, 10),
                            Resilience.timeoutMillis(1000))
                    .build()) {
                client.connect();
                assertEquals(Value.utf8("hello"), client.callFunction("getQuote", null));
                assertEquals(3, attempts.get(), "server attempts");
            }
        }
    }

    /** A fallback gives a default value in place of the failure. */
    // Example_fallback
    @Test
    void theFallbackOfTheExample() throws Exception {
        try (FakeServer server = new FakeServer("mem://resilience-fallback")) {
            server.function("risky", args -> {
                throw new VrpcException(Code.INTERNAL, "boom");
            });
            try (VrpcClient client = VrpcClient.builder().address(server.address())
                    .interceptors(Resilience.fallback((method, request, options, error) -> Value.utf8("cached")))
                    .build()) {
                client.connect();
                assertEquals(Value.utf8("cached"), client.callFunction("risky", null));
            }
        }
    }

    @Test
    void eachAttemptIsARequestOfItsOwnWithItsDeadline() throws Exception {
        try (FakeServer server = new FakeServer("tcp://127.0.0.1:0")) {
            flaky(server, "flaky", 2, "ok");
            try (VrpcClient client = VrpcClient.builder().address(server.address()).timeoutMillis(5000)
                    .interceptors(Resilience.retry().withBackoffMillis(1, 1), Resilience.timeoutMillis(750))
                    .build()) {
                assertEquals(Value.utf8("ok"), client.callFunction("flaky", Value.utf8("q")));
                for (long id = 1; id <= 3; id++) {
                    final MapValue request = server.next(2000);
                    assertEquals(id, D.requestId(request).longValue(), "a new request for each attempt");
                    assertEquals(750, D.slaMillis(request), "the timeout of the policy is the deadline the server is sent");
                    assertEquals(Value.utf8("q"), D.arguments(request));
                }
            }
        }
    }

    @Test
    void theBreakerOpensOnAServerThatIsGoneAndSparesIt() throws Exception {
        final FakeServer server = new FakeServer("tcp://127.0.0.1:0");
        final String address = server.address();
        server.close(); // nobody listens there any more
        try (VrpcClient client = VrpcClient.builder().address(address).dialTimeoutMillis(2000)
                .interceptors(Resilience.circuitBreaker().withFailureThreshold(3).withCooldownMillis(60_000))
                .build()) {
            for (int i = 0; i < 3; i++) {
                final VrpcException down = assertThrows(VrpcException.class, () -> client.callFunction("greet", null));
                assertEquals(Code.UNAVAILABLE, down.code());
                assertTrue(down.detail().startsWith("dial:"), down.detail());
            }
            final VrpcException open = assertThrows(VrpcException.class, () -> client.callFunction("greet", null));
            assertEquals("circuit breaker open for \"greet\"", open.detail(), "the fourth call does not dial");
        }
    }

    @Test
    void theTimeoutOfACallBoundsItsAttemptsOverAConnection() throws Exception {
        try (FakeServer server = new FakeServer("mem://resilience-budget")) {
            final AtomicInteger calls = new AtomicInteger();
            server.function("down", args -> {
                calls.incrementAndGet();
                throw new VrpcException(Code.UNAVAILABLE, "down");
            });
            try (VrpcClient client = VrpcClient.builder().address(server.address())
                    .interceptors(Resilience.retry().withMaxAttempts(1000).withBackoffMillis(20, 20).withJitter(false))
                    .build()) {
                final long start = System.nanoTime();
                final VrpcException last = assertThrows(VrpcException.class,
                        () -> client.callFunction("down", null, CallOptions.timeoutMillis(150)));
                final long took = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                assertEquals(Code.UNAVAILABLE, last.code(), "the last failure, not a timeout of its own");
                assertTrue(took < 2000, "it took " + took + " ms");
                assertTrue(calls.get() >= 2 && calls.get() <= 9, calls.get() + " attempts in 150 ms with 20 ms between them");
            }
        }
    }

    @Test
    void aCallMadeWithoutWaitingGoesThroughThePoliciesAndCanBeCancelled() throws Exception {
        try (FakeServer server = new FakeServer("mem://resilience-async")) {
            flaky(server, "flaky", 2, "ok");
            final CountDownLatch waiting = new CountDownLatch(1);
            final AtomicInteger attempts = new AtomicInteger();
            server.function("down", args -> {
                attempts.incrementAndGet();
                waiting.countDown();
                throw new VrpcException(Code.UNAVAILABLE, "down");
            });
            try (VrpcClient client = VrpcClient.builder().address(server.address()).timeoutMillis(5000)
                    .interceptors(Resilience.retry().withMaxAttempts(5).withBackoffMillis(1, 5))
                    .build()) {
                assertEquals(Value.utf8("ok"), client.callFunctionAsync("flaky", null, CallOptions.DEFAULT).get(10, TimeUnit.SECONDS));

                // One that is cancelled while its retry waits between attempts
                // makes no further attempt.
                final Retry slow = Resilience.retry().withMaxAttempts(5).withBackoffMillis(60_000, 60_000).withJitter(false);
                final CountDownLatch unwound = new CountDownLatch(1);
                final ClientInterceptor watch = (method, request, options, next) -> {
                    try {
                        return next.invoke(method, request, options);
                    } finally {
                        unwound.countDown();
                    }
                };
                try (VrpcClient patient = VrpcClient.builder().address(server.address()).timeoutMillis(5000)
                        .interceptors(watch, slow).build()) {
                    final CompletableFuture<Value> call = patient.callFunctionAsync("down", null, CallOptions.DEFAULT);
                    assertTrue(waiting.await(10, TimeUnit.SECONDS));
                    Thread.sleep(50); // the retry is in its wait
                    assertTrue(call.cancel(true));
                    assertThrows(CancellationException.class, call::join);
                    assertTrue(unwound.await(5, TimeUnit.SECONDS), "the retry stopped waiting when the call was cancelled");
                    assertEquals(1, attempts.get(), "no attempt after the cancellation");
                }
            } catch (ExecutionException e) {
                throw new AssertionError(e.getCause());
            }
        }
    }
}
