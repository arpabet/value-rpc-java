/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.resilience;

import com.arpabet.value.Value;
import com.arpabet.valuerpc.CallOptions;
import com.arpabet.valuerpc.ClientInterceptor;
import com.arpabet.valuerpc.Code;
import com.arpabet.valuerpc.VrpcException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The call at the end of a chain, for a test: it gives the outcomes it was
 * made with one after the other, the last one again and again, counts how
 * often it was called and keeps the options it was called with. It is the
 * {@code seqInvoker} of the Go tests.
 */
final class Calls implements ClientInterceptor.Invoker {

    private final Object[] outcomes;
    private final AtomicInteger count = new AtomicInteger();
    private final List<CallOptions> options = Collections.synchronizedList(new ArrayList<CallOptions>());

    private Calls(Object[] outcomes) {
        this.outcomes = outcomes;
    }

    /** @param outcomes each a {@link Value} to return or a {@link VrpcException} to throw; null returns nothing */
    static Calls of(Object... outcomes) {
        return new Calls(outcomes);
    }

    /** The outcome of a call that returned this text. */
    static Value ok(String text) {
        return Value.utf8(text);
    }

    /** The outcome of a call that failed with this code. */
    static VrpcException fail(Code code) {
        return new VrpcException(code, "x");
    }

    @Override
    public Value invoke(String method, Value request, CallOptions options) {
        final int n = Math.min(count.getAndIncrement(), outcomes.length - 1);
        this.options.add(options);
        final Object outcome = outcomes[n];
        if (outcome instanceof VrpcException) {
            throw (VrpcException) outcome;
        }
        if (outcome instanceof RuntimeException) {
            throw (RuntimeException) outcome;
        }
        return (Value) outcome;
    }

    /** How often it was called. */
    int count() {
        return count.get();
    }

    /** The timeout each call was made with, in the order of the calls. */
    List<Long> timeouts() {
        final List<Long> out = new ArrayList<Long>();
        synchronized (options) {
            for (CallOptions o : options) {
                out.add(o.timeoutMillis());
            }
        }
        return out;
    }
}
