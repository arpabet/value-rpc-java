/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.transport;

import java.util.concurrent.ScheduledThreadPoolExecutor;

/** The threads of the transports: daemons, so that none keeps a program alive. */
final class Threads {

    /** Runs what guards a write: nothing that takes time. */
    static final ScheduledThreadPoolExecutor TIMER;

    static {
        TIMER = new ScheduledThreadPoolExecutor(1, r -> daemon("vrpc-transport-timer", r));
        TIMER.setRemoveOnCancelPolicy(true);
        TIMER.setKeepAliveTime(30, java.util.concurrent.TimeUnit.SECONDS);
        TIMER.allowCoreThreadTimeOut(true);
    }

    private Threads() {
    }

    static Thread daemon(String name, Runnable task) {
        final Thread t = new Thread(task, name);
        t.setDaemon(true);
        return t;
    }
}
