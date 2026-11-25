/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * The golden files: what the Go implementation gives, written by
 * {@code conformance/golden}. They are read from the test resources, or from
 * the directory the system property {@code valuerpc.golden.dir} names (the
 * Gradle property {@code goldenDir}) when a larger set was written there.
 */
final class Golden {

    private Golden() {
    }

    /** The lines of a file, each split at its tabs. */
    static List<String[]> lines(String name) {
        final String dir = System.getProperty("valuerpc.golden.dir");
        try (InputStream in = dir != null
                ? Files.newInputStream(Paths.get(dir, name))
                : Golden.class.getResourceAsStream("/golden/" + name)) {
            if (in == null) {
                throw new IllegalStateException("no golden file " + name + ": run `go run ./golden` in conformance/");
            }
            final List<String[]> out = new ArrayList<String[]>();
            final BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            for (String line = r.readLine(); line != null; line = r.readLine()) {
                if (!line.isEmpty()) {
                    out.add(line.split("\t", -1));
                }
            }
            return out;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static byte[] unhex(String s) {
        final byte[] out = new byte[s.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16);
        }
        return out;
    }

    static String hex(byte[] b) {
        final StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte x : b) {
            sb.append(Character.forDigit(x >> 4 & 0xf, 16)).append(Character.forDigit(x & 0xf, 16));
        }
        return sb.toString();
    }

    /** The bytes a base64 column holds. */
    static byte[] bytes(String base64) {
        return Base64.getDecoder().decode(base64);
    }

    /** The text a base64 column holds. */
    static String text(String base64) {
        return new String(bytes(base64), StandardCharsets.UTF_8);
    }
}
