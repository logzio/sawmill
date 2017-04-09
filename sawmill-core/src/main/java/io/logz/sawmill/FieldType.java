package io.logz.sawmill;

import com.google.common.base.MoreObjects;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;

public enum FieldType {
    INT {
        @Override
        public Object convertFrom(Object value) {
            return MoreObjects.firstNonNull(Longs.tryParse(value.toString()), 0);
        }
    },
    LONG {
        @Override
        public Object convertFrom(Object value) {
            return MoreObjects.firstNonNull(Longs.tryParse(value.toString()), 0L);
        }
    },
    FLOAT {
        @Override
        public Object convertFrom(Object value) {
            return MoreObjects.firstNonNull(Doubles.tryParse(value.toString()), 0F);
        }
    },
    DOUBLE {
        @Override
        public Object convertFrom(Object value) {
            return MoreObjects.firstNonNull(Doubles.tryParse(value.toString()), 0D); }
    },
    STRING {
        @Override
        public Object convertFrom(Object value) {
            return value.toString();
        }
    },
    BOOLEAN {
        @Override
        public Object convertFrom(Object value) {
            if (value.toString().matches("^(t|true|yes|y|1)$")) {
                return true;
            } else if (value.toString().matches("^(f|false|no|n|0)$")) {
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
        Object valueAfterConvert = convertFrom(value);
        if (valueAfterConvert == null) return defaultValue;

        return valueAfterConvert;
    }
}
