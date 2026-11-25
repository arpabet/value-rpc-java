/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

/**
 * A QUIC transport for vRPC: the Go implementation's
 * {@code go.arpabet.com/value-rpc/quic}. It is a module of its own, so that
 * Netty and its native QUIC library come only with the programs that use
 * QUIC; the core stays free of them, and loads on Android, where this does
 * not.
 *
 * <p>{@link com.arpabet.valuerpc.quic.QuicTransport} makes the dialer of a
 * client and the listener of a server;
 * {@link com.arpabet.valuerpc.quic.QuicOptions} is what they are set up with.
 */
package com.arpabet.valuerpc.quic;
