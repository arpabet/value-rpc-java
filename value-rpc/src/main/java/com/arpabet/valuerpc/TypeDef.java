/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import com.arpabet.value.Kind;
import com.arpabet.value.ListValue;
import com.arpabet.value.MapValue;
import com.arpabet.value.Value;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * What the arguments or the result of a function must look like, checked when
 * a call arrives: vRPC has no schema, so the check is at run time. It is the
 * Go implementation's {@code valuerpc.TypeDef} and {@code Verify}.
 *
 * <pre>{@code
 * TypeDef.ANY                                           // anything
 * TypeDef.VOID                                          // nothing: null, or an empty list or map
 * TypeDef.STRING                                        // one string
 * TypeDef.list(TypeDef.STRING, TypeDef.NUMBER_OPT)      // a list of a string and, maybe, a number
 * TypeDef.map(TypeDef.param("id", Kind.NUMBER, true))   // a map with a number under "id"
 * }</pre>
 */
public abstract class TypeDef {

    /** Anything. */
    public static final TypeDef ANY = new AnyDef();

    /** Nothing: no value, the null value, or an empty list or map. */
    public static final TypeDef VOID = new VoidDef();

    public static final ArgDef BOOL = new ArgDef(Kind.BOOL, true);
    public static final ArgDef BOOL_OPT = new ArgDef(Kind.BOOL, false);
    public static final ArgDef NUMBER = new ArgDef(Kind.NUMBER, true);
    public static final ArgDef NUMBER_OPT = new ArgDef(Kind.NUMBER, false);
    public static final ArgDef STRING = new ArgDef(Kind.STRING, true);
    public static final ArgDef STRING_OPT = new ArgDef(Kind.STRING, false);

    TypeDef() {
    }

    /** One value of a kind, which may be left out unless it is required. */
    public static ArgDef arg(Kind kind, boolean required) {
        return new ArgDef(kind, required);
    }

    /** A list of exactly these values, in this order. */
    public static ArgsDef list(ArgDef... args) {
        return new ArgsDef(args);
    }

    /** A map with these entries; entries it does not name are let through. */
    public static ParamsDef map(ParamDef... params) {
        return new ParamsDef(params);
    }

    /** An entry of a map: a value of a kind under a name, which may be missing unless it is required. */
    public static ParamDef param(String name, Kind kind, boolean required) {
        return new ParamDef(name, kind, required);
    }

    /**
     * Reports whether a value is what this says it must be.
     *
     * @param value the arguments or the result; null where there is none,
     *              which is the same as the null value
     */
    public abstract boolean verify(Value value);

    static boolean absent(Value v) {
        return v == null || v.isNull();
    }

    private static final class AnyDef extends TypeDef {
        @Override
        public boolean verify(Value value) {
            return true;
        }

        @Override
        public String toString() {
            return "any";
        }
    }

    private static final class VoidDef extends TypeDef {
        @Override
        public boolean verify(Value value) {
            if (absent(value)) {
                return true;
            }
            switch (value.kind()) {
                case LIST:
                    return ((ListValue) value).size() == 0;
                case MAP:
                    return ((MapValue) value).isEmpty();
                default:
                    return false;
            }
        }

        @Override
        public String toString() {
            return "void";
        }
    }

    /** One value of a kind. */
    public static final class ArgDef extends TypeDef {
        private final Kind kind;
        private final boolean required;

        ArgDef(Kind kind, boolean required) {
            if (kind == null) {
                throw new NullPointerException("kind");
            }
            this.kind = kind;
            this.required = required;
        }

        public Kind kind() {
            return kind;
        }

        public boolean required() {
            return required;
        }

        /**
         * A value that is not required may be left out or be the null value: a
         * list has no holes, so null is how an argument before a given one is
         * skipped.
         */
        @Override
        public boolean verify(Value value) {
            if (absent(value)) {
                return !required;
            }
            return value.kind() == kind;
        }

        @Override
        public String toString() {
            return kind + (required ? "" : "?");
        }
    }

    /** A list of values, each of its kind. */
    public static final class ArgsDef extends TypeDef {
        private final ArgDef[] args;

        ArgsDef(ArgDef[] args) {
            this.args = args.clone();
        }

        public List<ArgDef> args() {
            return Collections.unmodifiableList(Arrays.asList(args));
        }

        @Override
        public boolean verify(Value value) {
            if (absent(value)) {
                return args.length == 0;
            }
            if (value.kind() != Kind.LIST) {
                return false;
            }
            final ListValue list = (ListValue) value;
            if (list.size() != args.length) {
                return false;
            }
            for (int i = 0; i < args.length; i++) {
                if (!args[i].verify(list.get(i))) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public String toString() {
            return Arrays.toString(args);
        }
    }

    /** A map with named entries, each of its kind. */
    public static final class ParamsDef extends TypeDef {
        private final ParamDef[] params;

        ParamsDef(ParamDef[] params) {
            this.params = params.clone();
        }

        public List<ParamDef> params() {
            return Collections.unmodifiableList(Arrays.asList(params));
        }

        @Override
        public boolean verify(Value value) {
            if (absent(value)) {
                return params.length == 0;
            }
            if (value.kind() != Kind.MAP) {
                return false;
            }
            final MapValue map = (MapValue) value;
            for (ParamDef p : params) {
                final Value v = map.get(p.name);
                if (v.isNull()) {
                    // Missing, or null: wrong only for an entry that is required.
                    if (p.required) {
                        return false;
                    }
                    continue;
                }
                if (v.kind() != p.kind) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public String toString() {
            return "{" + Arrays.toString(params) + "}";
        }
    }

    /** An entry of a {@link ParamsDef}. */
    public static final class ParamDef {
        private final String name;
        private final Kind kind;
        private final boolean required;

        ParamDef(String name, Kind kind, boolean required) {
            if (name == null || kind == null) {
                throw new NullPointerException(name == null ? "name" : "kind");
            }
            this.name = name;
            this.kind = kind;
            this.required = required;
        }

        public String name() {
            return name;
        }

        public Kind kind() {
            return kind;
        }

        public boolean required() {
            return required;
        }

        @Override
        public String toString() {
            return name + ": " + kind + (required ? "" : "?");
        }
    }
}
