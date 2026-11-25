/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

/**
 * The reverse hash chain a client proves the continuity of its session with
 * when it reconnects, so that a token caught on the way cannot be replayed.
 * It is the Go implementation's {@code valuerpc.HashChain}.
 *
 * <pre>
 *   seed = h[0],  h[i] = SHA-256(h[i-1]),  anchor = h[N]
 * </pre>
 *
 * The client sends the anchor with its first handshake and the server keeps
 * it. On every reconnect the client reveals the link before the last one it
 * revealed (h[N-1], then h[N-2], and so on). The server resumes the session
 * only if hashing what it is shown forward a few times gives the last link it
 * accepted, and then keeps what it was shown. Someone who saw a link cannot
 * compute the one before it, and a link shown again no longer hashes forward
 * to the stored one.
 *
 * <p>The client <em>always</em> moves on to the next link when it reconnects,
 * also when the handshake that carried a link was lost, so that a link is
 * never shown twice; the server makes up for the gap by hashing forward up to
 * {@link #DEFAULT_RESYNC_WINDOW} times.
 *
 * <p>This hardens resumption against someone who listens on a link without
 * TLS and replays. It is not a secure channel: it hides nothing, and does not
 * stop someone on the path who holds the real handshake back and shows the
 * same link first. Resumption is bound to the authenticated principal as
 * well. For confidentiality and integrity, use TLS.
 *
 * <p>A chain is safe to use from several threads.
 */
public final class HashChain {

    /**
     * The number of links of a chain, one per reconnect: after that many the
     * client has to start a new session. They take about 320 KB.
     */
    public static final int DEFAULT_LENGTH = 10000;

    /**
     * How many times a server hashes a link forward looking for the one it
     * holds: the number of lost handshakes in a row a chain recovers from,
     * and the most work a bogus link costs the server.
     */
    public static final int DEFAULT_RESYNC_WINDOW = 1024;

    private static final int LINK = 32;
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    /** links[0..32) is the seed, links[n*32..(n+1)*32) the anchor. */
    private final byte[] links;
    private final int length;
    /** The index of the link to reveal next; -1 when there is none left. */
    private int next;

    /** A chain of {@link #DEFAULT_LENGTH} links from a random seed. */
    public HashChain() {
        this(DEFAULT_LENGTH);
    }

    /** A chain of {@code n} links from a random seed; of {@link #DEFAULT_LENGTH} unless n is positive. */
    public HashChain(int n) {
        this(randomSeed(), n);
    }

    HashChain(byte[] seed, int n) {
        if (seed.length != LINK) {
            throw new IllegalArgumentException("a seed is " + LINK + " bytes");
        }
        if (n <= 0) {
            n = DEFAULT_LENGTH;
        }
        final MessageDigest sha = sha256();
        final byte[] all = new byte[(n + 1) * LINK];
        System.arraycopy(seed, 0, all, 0, LINK);
        for (int i = 1; i <= n; i++) {
            sha.update(all, (i - 1) * LINK, LINK);
            digestInto(sha, all, i * LINK);
        }
        this.links = all;
        this.length = n;
        this.next = n - 1;
    }

    private static byte[] randomSeed() {
        final byte[] seed = new byte[LINK];
        new SecureRandom().nextBytes(seed);
        return seed;
    }

    /**
     * The anchor h[N], in hexadecimal: what the first handshake carries. It
     * may be sent again when a first connect is retried; sending it takes no
     * link.
     */
    public String anchor() {
        return hex(links, length * LINK);
    }

    /**
     * The next link to reveal, in hexadecimal, and moves on: a link is given
     * out once. Null when the chain is used up and a new session is needed.
     */
    public synchronized String next() {
        if (next < 0) {
            return null;
        }
        return hex(links, next-- * LINK);
    }

    /** How many links are left. */
    public synchronized int remaining() {
        return next + 1;
    }

    /**
     * Whether {@code presented} is a link that comes before
     * {@code lastVerified} in a chain, no more than {@code window} links
     * before it: the number of times it had to be hashed to give
     * {@code lastVerified}, at least 1, or 0 when it is not. The caller then
     * keeps {@code presented} as the last link verified. A link shown again,
     * or {@code lastVerified} itself, is not one.
     *
     * @param window how far to look; {@link #DEFAULT_RESYNC_WINDOW} unless positive
     */
    public static int verifyStep(String presented, String lastVerified, int window) {
        final byte[] cur = decode(presented);
        final byte[] target = decode(lastVerified);
        if (cur == null || target == null) {
            return 0;
        }
        if (window <= 0) {
            window = DEFAULT_RESYNC_WINDOW;
        }
        final MessageDigest sha = sha256();
        byte[] h = cur;
        for (int step = 1; step <= window; step++) {
            h = sha.digest(h);
            if (MessageDigest.isEqual(h, target)) {
                return step;
            }
        }
        return 0;
    }

    /**
     * Whether two links are the same, compared in constant time. A server
     * takes the link it holds, shown again, as the retry of a handshake whose
     * answer was lost. A link that is empty or malformed equals nothing.
     */
    public static boolean sameToken(String a, String b) {
        final byte[] da = decode(a);
        final byte[] db = decode(b);
        return da != null && db != null && MessageDigest.isEqual(da, db);
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("no SHA-256", e);
        }
    }

    private static void digestInto(MessageDigest sha, byte[] out, int offset) {
        try {
            sha.digest(out, offset, LINK);
        } catch (java.security.DigestException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String hex(byte[] b, int offset) {
        final char[] out = new char[LINK * 2];
        for (int i = 0; i < LINK; i++) {
            final int v = b[offset + i] & 0xff;
            out[i * 2] = HEX[v >>> 4];
            out[i * 2 + 1] = HEX[v & 0xf];
        }
        return new String(out);
    }

    /** The bytes of a link in hexadecimal, of either case; null unless it is one. */
    private static byte[] decode(String s) {
        if (s == null || s.length() != LINK * 2) {
            return null;
        }
        final byte[] out = new byte[LINK];
        for (int i = 0; i < LINK; i++) {
            final int hi = Character.digit(s.charAt(i * 2), 16);
            final int lo = Character.digit(s.charAt(i * 2 + 1), 16);
            if (hi < 0 || lo < 0 || s.charAt(i * 2) > 'f' || s.charAt(i * 2 + 1) > 'f') {
                return null;
            }
            out[i] = (byte) (hi << 4 | lo);
        }
        return out;
    }
}
