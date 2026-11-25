/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import com.arpabet.value.Value;

/**
 * Serves a call: takes the arguments and gives the result. It is the Go
 * implementation's {@code valuerpc.Function}.
 *
 * <pre>{@code
 * server.addFunction("greet", (context, args) ->
 *         Value.utf8("Hello, " + ((ListValue) args).get(0) + "!"));
 * }</pre>
 *
 * A handler runs on a thread of its own for each call, and may wait. It is
 * interrupted when the call is cancelled or its time has passed (see
 * {@link RequestContext#isCancelled}).
 */
@FunctionalInterface
public interface UnaryHandler {

    /**
     * @param args the arguments; the null value when the caller sent none
     * @return the result; null, or the null value, for none
     * @throws Exception the call failed. A {@link VrpcException} chooses the
     *         code the caller sees; any other failure is {@link Code#INTERNAL},
     *         with its message
     */
    Value handle(RequestContext context, Value args) throws Exception;
}
