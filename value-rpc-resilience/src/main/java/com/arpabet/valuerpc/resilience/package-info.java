/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

/**
 * Service-governance policies for vRPC clients as composable interceptors
 * ({@link com.arpabet.valuerpc.ClientInterceptor}): retry, circuit breaking,
 * timeouts, rate limiting, bulkheading and fallback. It is the Go
 * implementation's {@code go.arpabet.com/value-rpc/resilience}, with the same
 * policies, defaults and failures.
 *
 * <p>It is a module of its own, so that these policies never burden a plain
 * client: the core has only the seam, and the policies are here. They are
 * installed with the interceptors of a client:
 *
 * <pre>{@code
 * VrpcClient client = VrpcClient.builder()
 *         .address("tcp://example.com:9000")
 *         .interceptors(
 *                 Resilience.rateLimit(100, 20),                 // outermost
 *                 Resilience.circuitBreaker(),
 *                 Resilience.retry().withMaxAttempts(3),
 *                 Resilience.timeoutMillis(2000))                // innermost: each attempt
 *         .build();
 * }</pre>
 *
 * <p>The order matters: the first interceptor is the outermost. A typical
 * chain is rate limit, circuit breaker, retry, timeout, call, so that the
 * breaker sees whole operations, after their retries, and the timeout bounds
 * each attempt.
 *
 * <p>The policies tell outcomes apart by the {@link com.arpabet.valuerpc.Code}
 * of the failure. A retry by default tries again only after
 * {@code UNAVAILABLE} and {@code RESOURCE_EXHAUSTED}: codes that say the
 * server did not process the request, so that another try is safe also for a
 * call that is not idempotent. Widening that set to codes after which the
 * call may have run makes the retry at-least-once: do so only for idempotent
 * methods (see {@link com.arpabet.valuerpc.resilience.Retry#withRetryable}).
 *
 * <p>Where the Go implementation takes a {@code context.Context}, a call here
 * has its {@link com.arpabet.valuerpc.CallOptions} and its thread. A timeout
 * the caller gave with the options is the deadline of the context: it bounds
 * the whole call, all its attempts and the waits between them included, and a
 * policy that waits gives up when it is used up. An interrupt of the calling
 * thread is the cancellation of the context: a policy that waits stops
 * waiting, and the thread stays interrupted.
 *
 * <p>The policies wrap calls, as the interceptors do; streams do not go
 * through them.
 */
package com.arpabet.valuerpc.resilience;
