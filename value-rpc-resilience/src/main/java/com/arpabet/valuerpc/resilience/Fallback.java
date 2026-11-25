/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.resilience;

import com.arpabet.value.Value;
import com.arpabet.valuerpc.CallOptions;
import com.arpabet.valuerpc.VrpcException;

/**
 * What answers in place of a call that failed: see
 * {@link Resilience#fallback}.
 */
@FunctionalInterface
public interface Fallback {

    /**
     * @param failure what the call failed with
     * @return the result the caller gets instead: a cached or a default
     *         value, say; null for a call that returns nothing
     * @throws VrpcException the failure the caller gets instead: the one
     *         given, or another made of it
     */
    Value apply(String method, Value request, CallOptions options, VrpcException failure);
}
