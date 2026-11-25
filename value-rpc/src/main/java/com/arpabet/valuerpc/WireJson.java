/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import com.arpabet.value.Limits;
import com.arpabet.value.NullValue;
import com.arpabet.value.StringType;
import com.arpabet.value.StringValue;
import com.arpabet.value.Value;
import com.arpabet.value.ValueException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;

/**
 * Reads the JSON of the JSON wire codec, which is the JSON
 * {@link Value#jsonify()} writes, back into a value. It is the Go
 * implementation's {@code valuerpc.ParseJson}, and reads what that reads:
 *
 * <ul>
 * <li>null, true, false: the null value, a boolean;
 * <li>a number written as an integer: a long, or a big integer when it does
 *     not fit one;
 * <li>a number with a fraction or an exponent: a double;
 * <li>a string that starts with {@code base64,}: binary;
 * <li>a string that starts with the prefix of an extension: that extension;
 * <li>any other string: text;
 * <li>an array, an object: a list, a map (of a key that repeats, the last).
 * </ul>
 *
 * A string is never given a type by what it says, so a big integer, a decimal
 * and a timestamp, which {@code jsonify} writes as strings, are read as text.
 *
 * <p>This is not {@link Value#parseJson}, which reads the lossless JSON of
 * {@link Value#toJson} and takes a bare number for a decimal.
 */
final class WireJson {

    /** The depth at which Go's JSON decoder gives up, whatever the limits say. */
    private static final int HARD_DEPTH = 10000;

    private final byte[] b;
    private final int n;
    private final Limits limits;
    private int i;
    /** The length in UTF-8 of the string {@link #string} read last. */
    private long stringBytes;

    private WireJson(byte[] data, Limits limits) {
        this.b = data;
        this.n = data.length;
        this.limits = limits;
    }

    /**
     * Reads a value from JSON in UTF-8.
     *
     * @throws ValueException the text is not JSON, or exceeds a limit
     */
    static Value parse(byte[] data, Limits limits) {
        final WireJson p = new WireJson(data, limits);
        try {
            p.space();
            final Value v = p.value(0);
            p.space();
            // What follows the value is an error, as it is to Go's decoder,
            // which does not count a closing bracket or brace as following.
            if (p.i < p.n && p.b[p.i] != ']' && p.b[p.i] != '}') {
                throw new ValueException("value: trailing data after JSON value");
            }
            return v;
        } catch (StackOverflowError e) {
            throw new ValueException("value: json is nested too deeply");
        }
    }

    private ValueException error(String what) {
        return new ValueException("value: json decode: " + what + " at offset " + i);
    }

    private void space() {
        while (i < n) {
            final byte c = b[i];
            if (c != ' ' && c != '\t' && c != '\r' && c != '\n') {
                return;
            }
            i++;
        }
    }

    private Value value(int depth) {
        final int maxDepth = limits.maxDepth();
        if (maxDepth > 0 && depth > maxDepth) {
            throw new ValueException("value: max parse depth " + maxDepth + " exceeded");
        }
        if (depth > HARD_DEPTH) {
            throw error("exceeded max depth");
        }
        if (i >= n) {
            throw error("unexpected end of JSON input");
        }
        final byte c = b[i];
        switch (c) {
            case '{':
                return object(depth);
            case '[':
                return array(depth);
            case '"':
                return text(string());
            case 't':
                literal("true");
                return Value.ofBool(true);
            case 'f':
                literal("false");
                return Value.ofBool(false);
            case 'n':
                literal("null");
                return NullValue.NULL;
            default:
                if (c == '-' || (c >= '0' && c <= '9')) {
                    return number();
                }
                throw error("invalid character looking for beginning of value");
        }
    }

    private void literal(String word) {
        final int len = word.length();
        if (i + len > n) {
            throw error("unexpected end of JSON input");
        }
        for (int k = 0; k < len; k++) {
            if (b[i + k] != word.charAt(k)) {
                throw error("invalid character in literal " + word);
            }
        }
        i += len;
    }

    private Value array(int depth) {
        i++;
        final int max = limits.maxCollectionLength();
        final ArrayList<Value> items = new ArrayList<Value>();
        space();
        if (i < n && b[i] == ']') {
            i++;
            return Value.listOf(items);
        }
        for (;;) {
            space();
            items.add(value(depth + 1));
            if (max > 0 && items.size() > max) {
                throw new ValueException("value: json array length exceeds limit " + max);
            }
            space();
            if (i >= n) {
                throw error("unexpected end of JSON input");
            }
            final byte c = b[i++];
            if (c == ']') {
                return Value.listOf(items);
            }
            if (c != ',') {
                i--;
                throw error("invalid character after array element");
            }
        }
    }

    private Value object(int depth) {
        i++;
        final int max = limits.maxCollectionLength();
        final LinkedHashMap<String, Value> entries = new LinkedHashMap<String, Value>();
        space();
        if (i < n && b[i] == '}') {
            i++;
            return Value.mapOf(entries);
        }
        for (;;) {
            space();
            if (i >= n) {
                throw error("unexpected end of JSON input");
            }
            if (b[i] != '"') {
                throw error("invalid character looking for beginning of object key string");
            }
            final String key = string();
            space();
            if (i >= n) {
                throw error("unexpected end of JSON input");
            }
            if (b[i] != ':') {
                throw error("invalid character after object key");
            }
            i++;
            space();
            // Of a key that repeats, the last value is the one.
            entries.put(key, value(depth + 1));
            if (max > 0 && entries.size() > max) {
                throw new ValueException("value: json object size exceeds limit " + max);
            }
            space();
            if (i >= n) {
                throw error("unexpected end of JSON input");
            }
            final byte c = b[i++];
            if (c == '}') {
                return Value.mapOf(entries);
            }
            if (c != ',') {
                i--;
                throw error("invalid character after object key:value pair");
            }
        }
    }

    private Value number() {
        final int start = i;
        if (b[i] == '-') {
            i++;
        }
        if (i >= n) {
            throw error("unexpected end of JSON input");
        }
        if (b[i] == '0') {
            i++;
        } else if (b[i] >= '1' && b[i] <= '9') {
            digits();
        } else {
            throw error("invalid character in numeric literal");
        }
        boolean integer = true;
        if (i < n && b[i] == '.') {
            integer = false;
            i++;
            if (i >= n || b[i] < '0' || b[i] > '9') {
                throw error("invalid character after decimal point in numeric literal");
            }
            digits();
        }
        if (i < n && (b[i] == 'e' || b[i] == 'E')) {
            integer = false;
            i++;
            if (i < n && (b[i] == '+' || b[i] == '-')) {
                i++;
            }
            if (i >= n || b[i] < '0' || b[i] > '9') {
                throw error("invalid character in exponent of numeric literal");
            }
            digits();
        }
        final String s = new String(b, start, i - start, StandardCharsets.US_ASCII);
        if (integer) {
            // Written as an integer: a long, and a big integer beyond one, so
            // that no digit is lost.
            try {
                return Value.ofLong(Long.parseLong(s));
            } catch (NumberFormatException e) {
                return Value.ofBigInt(new BigInteger(s));
            }
        }
        final double d = Double.parseDouble(s);
        if (Double.isInfinite(d)) {
            throw new ValueException("value: invalid json number " + s + ": out of range");
        }
        return Value.ofDouble(d);
    }

    private void digits() {
        while (i < n && b[i] >= '0' && b[i] <= '9') {
            i++;
        }
    }

    /** What a JSON string is as a value: binary, an extension, or text. */
    private Value text(String s) {
        final int max = limits.maxByteLength();
        if (max > 0 && stringBytes > max) {
            throw new ValueException("value: json string length " + stringBytes + " exceeds limit " + max);
        }
        final String ext = Value.unknownPrefix();
        if (s.startsWith(ext)) {
            final String rest = s.substring(ext.length());
            if (rest.startsWith(Value.base64Prefix())) {
                final StringValue raw = StringValue.parse(rest);
                if (raw.type() == StringType.RAW) {
                    final byte[] payload = raw.raw();
                    if (payload.length >= 1) {
                        try {
                            return Value.ext(payload[0], Arrays.copyOfRange(payload, 1, payload.length));
                        } catch (ValueException e) {
                            // Not the payload of the kind its type names: the text it is.
                        }
                    }
                }
            }
            return Value.utf8(s);
        }
        // The base64 prefix makes it the binary it was written from.
        return StringValue.parse(s);
    }

    /**
     * Reads the string at the quote it stands on. A byte that is not UTF-8,
     * and a surrogate escape without its pair, is U+FFFD, as Go reads it.
     */
    private String string() {
        i++;
        final StringBuilder sb = new StringBuilder();
        long bytes = 0;
        for (;;) {
            if (i >= n) {
                throw error("unexpected end of JSON input");
            }
            final int c = b[i] & 0xff;
            if (c == '"') {
                i++;
                break;
            }
            if (c < 0x20) {
                throw error("invalid character in string literal");
            }
            if (c == '\\') {
                i++;
                if (i >= n) {
                    throw error("unexpected end of JSON input");
                }
                final int e = b[i++] & 0xff;
                switch (e) {
                    case '"':
                    case '\\':
                    case '/':
                        sb.append((char) e);
                        bytes++;
                        break;
                    case 'b':
                        sb.append('\b');
                        bytes++;
                        break;
                    case 'f':
                        sb.append('\f');
                        bytes++;
                        break;
                    case 'n':
                        sb.append('\n');
                        bytes++;
                        break;
                    case 'r':
                        sb.append('\r');
                        bytes++;
                        break;
                    case 't':
                        sb.append('\t');
                        bytes++;
                        break;
                    case 'u':
                        bytes += escaped(sb);
                        break;
                    default:
                        i--;
                        throw error("invalid character in string escape code");
                }
                continue;
            }
            if (c < 0x80) {
                sb.append((char) c);
                bytes++;
                i++;
                continue;
            }
            final int at = i;
            final int cp = utf8();
            if (cp < 0) {
                sb.append((char) 0xFFFD);
                bytes += 3;
            } else {
                sb.appendCodePoint(cp);
                bytes += i - at;
            }
        }
        stringBytes = bytes;
        return sb.toString();
    }

    /**
     * Reads the four digits of a Unicode escape, and the escape after it when
     * the two are a surrogate pair. Returns the length in UTF-8 of what it
     * appended.
     */
    private int escaped(StringBuilder sb) {
        final int u = hex4(i);
        if (u < 0) {
            throw error("invalid character in \\u hexadecimal character escape");
        }
        i += 4;
        if (u < 0xD800 || u > 0xDFFF) {
            sb.append((char) u);
            return u < 0x80 ? 1 : u < 0x800 ? 2 : 3;
        }
        if (u <= 0xDBFF && i + 6 <= n && b[i] == '\\' && b[i + 1] == 'u') {
            final int low = hex4(i + 2);
            if (low >= 0xDC00 && low <= 0xDFFF) {
                i += 6;
                sb.append((char) u).append((char) low);
                return 4;
            }
        }
        // A surrogate without its pair.
        sb.append((char) 0xFFFD);
        return 3;
    }

    /** The number of the four hexadecimal digits at an offset, or -1 when they are not there. */
    private int hex4(int at) {
        if (at + 4 > n) {
            return -1;
        }
        int u = 0;
        for (int k = 0; k < 4; k++) {
            final int c = b[at + k];
            final int d;
            if (c >= '0' && c <= '9') {
                d = c - '0';
            } else if (c >= 'a' && c <= 'f') {
                d = c - 'a' + 10;
            } else if (c >= 'A' && c <= 'F') {
                d = c - 'A' + 10;
            } else {
                return -1;
            }
            u = u << 4 | d;
        }
        return u;
    }

    /**
     * Reads the character in UTF-8 at the offset and moves past it. A byte
     * that does not begin one is passed alone, and -1 returned.
     */
    private int utf8() {
        final int c0 = b[i] & 0xff;
        final int more;
        final int min;
        int cp;
        if (c0 >= 0xC2 && c0 <= 0xDF) {
            more = 1;
            min = 0x80;
            cp = c0 & 0x1f;
        } else if (c0 >= 0xE0 && c0 <= 0xEF) {
            more = 2;
            min = 0x800;
            cp = c0 & 0x0f;
        } else if (c0 >= 0xF0 && c0 <= 0xF4) {
            more = 3;
            min = 0x10000;
            cp = c0 & 0x07;
        } else {
            i++;
            return -1;
        }
        if (i + more >= n) {
            i++;
            return -1;
        }
        for (int k = 1; k <= more; k++) {
            final int c = b[i + k] & 0xff;
            if ((c & 0xC0) != 0x80) {
                i++;
                return -1;
            }
            cp = cp << 6 | (c & 0x3f);
        }
        if (cp < min || cp > 0x10FFFF || (cp >= 0xD800 && cp <= 0xDFFF)) {
            i++;
            return -1;
        }
        i += more + 1;
        return cp;
    }
}
