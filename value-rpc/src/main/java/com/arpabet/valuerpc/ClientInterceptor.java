/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import com.arpabet.value.Value;
import java.util.List;

/**
 * Wraps a call a client makes. It may look at or change what is sent, answer
 * without calling on (a circuit breaker that is open, a rate limit), call on
 * more than once (a retry), and look at or change what comes back (a
 * fallback). Interceptors are where the policies of a service plug in; the
 * library has the seam, and the policies are apart from it. It is the Go
 * implementation's {@code valuerpc.ClientInterceptor}.
 *
 * <p>An interceptor tells failures apart by their {@link Code}:
 * {@code UNAVAILABLE}, {@code DEADLINE_EXCEEDED} and
 * {@code RESOURCE_EXHAUSTED} are usually worth another try;
 * {@code INVALID_ARGUMENT}, {@code NOT_FOUND} and {@code UNAUTHENTICATED} are
 * not.
 */
@FunctionalInterface
public interface ClientInterceptor {

    /**
     * @param next the rest of the chain, the call itself at its end
     * @return the result of the call; null when it returned nothing
     * @throws VrpcException the call failed
     */
    Value intercept(String method, Value request, CallOptions options, Invoker next);

    /** Makes a call: the next step of a chain of interceptors. */
    @FunctionalInterface
    interface Invoker {

        /**
         * @return the result of the call; null when it returned nothing
         * @throws VrpcException the call failed
         */
        Value invoke(String method, Value request, CallOptions options);
    }

    /**
     * The interceptors around a call as one invoker. The first is the
     * outermost: it runs first and wraps the rest. None gives the call itself.
     */
    static Invoker chain(Invoker call, List<ClientInterceptor> interceptors) {
        Invoker invoker = call;
        for (int i = interceptors.size() - 1; i >= 0; i--) {
            final ClientInterceptor interceptor = interceptors.get(i);
            final Invoker next = invoker;
            invoker = (method, request, options) -> interceptor.intercept(method, request, options, next);
        }
        return invoker;
    }
}
