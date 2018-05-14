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
            return Doubles.tryParse(String.valueOf(value));
        }
    },
    DOUBLE {
        @Override
        public Object convertFrom(Object value) {
            return Doubles.tryParse(String.valueOf(value)); }
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
            if (String.valueOf(value).matches("^(t|true|yes|y|1)$")) {
                return true;
            } else if (String.valueOf(value).matches("^(f|false|no|n|0)$")) {
                return false;
            } else {
                return null;
            }
        }
    };

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
