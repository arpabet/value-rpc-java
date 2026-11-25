/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.arpabet.value.Kind;
import com.arpabet.value.Limits;
import com.arpabet.value.Value;
import com.arpabet.value.ValueException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The parts of the protocol that are not envelopes, against what the Go
 * implementation gives for them: the codes, the hash chain, the verification
 * of arguments and the reading of the JSON wire codec.
 */
class ProtocolGoldenTest {

    @Test
    void codesHaveTheNamesAndTheTextOfGo() {
        int n = 0;
        for (String[] f : Golden.lines("codes.tsv")) {
            final long number = Long.parseLong(f[0]);
            assertEquals(f[1], Code.of(number).toString(), "the name of the code " + number);
            assertEquals(Golden.text(f[2]), VrpcException.ofNumber(number, "boom").getMessage(), "the text of " + number);
            if (number >= 0 && number <= 9) {
                assertEquals(number, Code.of(number).number());
                assertEquals(Golden.text(f[2]), new VrpcException(Code.of(number), "boom").getMessage());
            } else {
                assertEquals(Code.UNKNOWN, Code.of(number));
                assertEquals(number, VrpcException.ofNumber(number, "boom").codeNumber(), "the number is kept");
            }
            n++;
        }
        assertTrue(n >= 10);
    }

    @Test
    void hashChainIsTheChainOfGo() {
        int chains = 0;
        int verified = 0;
        for (String[] f : Golden.lines("hashchain.tsv")) {
            switch (f[0]) {
                case "chain": {
                    final int n = Integer.parseInt(f[2]);
                    final HashChain chain = new HashChain(Golden.unhex(f[1]), n);
                    assertEquals(f[3], chain.anchor(), "the anchor");
                    assertEquals(f[3], chain.anchor(), "asking for the anchor takes no link");
                    final String[] links = f[4].split(",");
                    assertEquals(n, links.length);
                    String last = chain.anchor();
                    for (int i = 0; i < links.length; i++) {
                        assertEquals(n - i, chain.remaining());
                        final String link = chain.next();
                        assertEquals(links[i], link, "link " + i);
                        assertEquals(1, HashChain.verifyStep(link, last, 0), "each link is one step before the last");
                        last = link;
                    }
                    assertEquals(0, chain.remaining());
                    assertNull(chain.next(), "a chain that is used up gives no link");
                    chains++;
                    break;
                }
                case "verify":
                    assertEquals(Integer.parseInt(f[4]), HashChain.verifyStep(f[1], f[2], Integer.parseInt(f[3])),
                            "verify " + f[1] + " against " + f[2] + " within " + f[3]);
                    verified++;
                    break;
                case "same":
                    assertEquals(Boolean.parseBoolean(f[3]), HashChain.sameToken(f[1], f[2]), f[1] + " = " + f[2]);
                    break;
                default:
                    fail("hashchain.tsv: " + f[0]);
            }
        }
        assertTrue(chains > 0 && verified > 0);
    }

    /** The type definition a column of {@code verify.tsv} describes. */
    private static TypeDef typeDef(String spec) {
        if (spec.equals("any")) {
            return TypeDef.ANY;
        }
        if (spec.equals("void")) {
            return TypeDef.VOID;
        }
        final int colon = spec.indexOf(':');
        final String form = spec.substring(0, colon);
        final String rest = spec.substring(colon + 1);
        switch (form) {
            case "arg":
                return arg(rest);
            case "list": {
                final List<TypeDef.ArgDef> args = new ArrayList<TypeDef.ArgDef>();
                if (!rest.isEmpty()) {
                    for (String a : rest.split(",")) {
                        args.add(arg(a));
                    }
                }
                return TypeDef.list(args.toArray(new TypeDef.ArgDef[0]));
            }
            case "map": {
                final List<TypeDef.ParamDef> params = new ArrayList<TypeDef.ParamDef>();
                if (!rest.isEmpty()) {
                    for (String p : rest.split(",")) {
                        final int eq = p.indexOf('=');
                        final TypeDef.ArgDef a = arg(p.substring(eq + 1));
                        params.add(TypeDef.param(p.substring(0, eq), a.kind(), a.required()));
                    }
                }
                return TypeDef.map(params.toArray(new TypeDef.ParamDef[0]));
            }
            default:
                throw new AssertionError(spec);
        }
    }

    private static TypeDef.ArgDef arg(String s) {
        final String[] p = s.split(":");
        return TypeDef.arg(Kind.valueOf(p[0]), p[1].equals("req"));
    }

    @Test
    void verificationOfArgumentsIsGos() {
        int n = 0;
        for (String[] f : Golden.lines("verify.tsv")) {
            final Value v = f[1].equals("nil") ? null : Value.unpack(Golden.unhex(f[1]));
            assertEquals(Boolean.parseBoolean(f[2]), typeDef(f[0]).verify(v),
                    f[0] + " of " + (v == null ? "no value" : v.jsonify()));
            n++;
        }
        assertTrue(n > 100);
    }

    @Test
    void jsonIsReadAsGoReadsIt() {
        int read = 0;
        int refused = 0;
        for (String[] f : Golden.lines("json.tsv")) {
            final byte[] json = Golden.bytes(f[0]);
            final String shown = Golden.text(f[0]);
            if (f[1].equals("error")) {
                try {
                    final Value v = WireJson.parse(json, Limits.DEFAULT);
                    fail("Go refuses " + shown + ", read here as " + v.jsonify());
                } catch (ValueException expected) {
                    refused++;
                }
                continue;
            }
            final Value v;
            try {
                v = WireJson.parse(json, Limits.DEFAULT);
            } catch (ValueException e) {
                throw new AssertionError("Go reads " + (shown.length() > 200 ? shown.substring(0, 200) + "..." : shown)
                        + ", refused here: " + e.getMessage(), e);
            }
            assertEquals(f[1], v.hex(), "what " + (shown.length() > 200 ? shown.substring(0, 200) + "..." : shown) + " is");
            read++;
        }
        assertTrue(read > 100 && refused > 20, read + " read, " + refused + " refused");
    }
}
