package io.logz.sawmill;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class FieldTypeTest {

    @Test
    public void testBooleanMatchesPreviousRegexes() {
        List<Object> values = Arrays.asList("t", "true", "yes", "y", "1", "f", "false", "no", "n", "0",
                "True", "TRUE", "Y", " true", "true ", "true\n", "\ntrue", "", "2", "yess", "tru", "1.0", 1, 0, 1L, true, false, null);
        for (Object value : values) {
            String text = String.valueOf(value);
            Object expected = text.matches("^(t|true|yes|y|1)$") ? Boolean.TRUE : text.matches("^(f|false|no|n|0)$") ? Boolean.FALSE : null;
            assertThat(FieldType.BOOLEAN.convertFrom(value)).as("[%s]", text).isEqualTo(expected);
        }
    }

    @Test
    public void testDoubleMatchesGuava() {
        List<Object> values = new java.util.ArrayList<>(Arrays.asList("1", "-1", "1.5", "-0", "0.0", "1.", ".5", "1e5", "+1",
                " 1", "1 ", "NaN", "Infinity", "-Infinity", "0x1p3", "1d", "1f", "\u0661\u0662", "", "-", "--1", "1.2.3",
                "007", "12345678901234567890123", "0.30000000000000004", "1x", 5, -5L, 1.5, 1e20, null));
        java.util.Random random = new java.util.Random(17);
        String alphabet = "0123456789-+.eE xXpabfdNIn";
        for (int n = 0; n < 50000; n++) {
            StringBuilder text = new StringBuilder();
            for (int c = random.nextInt(12); c > 0; c--) text.append(alphabet.charAt(random.nextInt(alphabet.length())));
            values.add(text.toString());
        }
        for (Object value : values) {
            Double expected = com.google.common.primitives.Doubles.tryParse(String.valueOf(value));
            assertThat(FieldType.DOUBLE.convertFrom(value)).as("[%s]", value).isEqualTo(expected);
            assertThat(FieldType.FLOAT.convertFrom(value)).as("[%s]", value).isEqualTo(expected);
        }
    }
}
