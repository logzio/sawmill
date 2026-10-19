package io.logz.sawmill;

import com.google.common.base.MoreObjects;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;

public enum FieldType {
    INT {
        @Override
        public Object convertFrom(Object value) {
            return Longs.tryParse(String.valueOf(value));
        }
    },
    LONG {
        @Override
        public Object convertFrom(Object value) {
            return Longs.tryParse(String.valueOf(value));
        }
    },
    FLOAT {
        @Override
        public Object convertFrom(Object value) {
            return parseDouble(String.valueOf(value));
        }
    },
    DOUBLE {
        @Override
        public Object convertFrom(Object value) {
            return parseDouble(String.valueOf(value)); }
    },
    STRING {
        @Override
        public Object convertFrom(Object value) {
            return String.valueOf(value);
        }
    },
    BOOLEAN {
        @Override
        public Object convertFrom(Object value) {
            // exactly the strings ^(t|true|yes|y|1)$ and ^(f|false|no|n|0)$ matched
            switch (String.valueOf(value)) {
                case "t": case "true": case "yes": case "y": case "1":
                    return true;
                case "f": case "false": case "no": case "n": case "0":
                    return false;
                default:
                    return null;
            }
        }
    };

    /**
     * {@code Doubles.tryParse}, which checks every value with a regex first. Plain decimals ({@code -digits.digits},
     * the sign and fraction optional) pass that check and end in {@code Double.parseDouble}, so they go there directly.
     */
    private static Double parseDouble(String text) {
        return isPlainDecimal(text) ? Double.valueOf(Double.parseDouble(text)) : Doubles.tryParse(text);
    }

    private static boolean isPlainDecimal(String text) {
        int length = text.length();
        int i = length > 0 && text.charAt(0) == '-' ? 1 : 0;
        int integerStart = i;
        while (i < length && isDigit(text.charAt(i))) i++;
        if (i == integerStart) return false;
        if (i == length) return true;
        if (text.charAt(i) != '.') return false;
        int fractionStart = ++i;
        while (i < length && isDigit(text.charAt(i))) i++;
        return i > fractionStart && i == length;
    }

    private static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }

    @Override
    public String toString() {
        return this.name().toLowerCase();
    }

    public static FieldType tryParseOrDefault(String type) {
        try {
            return FieldType.valueOf(type.toUpperCase());
        } catch (IllegalArgumentException e) {
            return STRING;
        }
    }

    public abstract Object convertFrom(Object value);

    public Object convertFrom(Object value, Object defaultValue) {
        return MoreObjects.firstNonNull(convertFrom(value), defaultValue);
    }
}
