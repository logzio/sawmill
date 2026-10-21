package io.logz.sawmill.processors;

import java.time.ZonedDateTime;

/**
 * Writes a date the way {@link DateProcessor#ELASTIC} formats it: {@code yyyy-MM-ddTHH:mm:ss.SSS} (milliseconds
 * truncated) followed by the offset id ({@code Z}, {@code +HH:MM}, or {@code +HH:MM:ss} when the offset has seconds).
 * Years 0 to 9999, which print as four digits without a sign, are written without java.time's formatter.
 */
final class ElasticDateFormat {

    private ElasticDateFormat() {
    }

    static String format(ZonedDateTime dateTime) {
        int year = dateTime.getYear();
        if (year < 0 || year > 9999) return dateTime.format(DateProcessor.ELASTIC);

        StringBuilder out = new StringBuilder(32);
        digits(out, year, 4).append('-');
        digits(out, dateTime.getMonthValue(), 2).append('-');
        digits(out, dateTime.getDayOfMonth(), 2).append('T');
        digits(out, dateTime.getHour(), 2).append(':');
        digits(out, dateTime.getMinute(), 2).append(':');
        digits(out, dateTime.getSecond(), 2).append('.');
        digits(out, dateTime.getNano() / 1_000_000, 3);
        // the same text as the formatter's offset id
        return out.append(dateTime.getOffset().getId()).toString();
    }

    /** {@code value} (non-negative) zero-padded to {@code width} digits. */
    private static StringBuilder digits(StringBuilder out, int value, int width) {
        for (int limit = 10; width > 1; limit *= 10, width--) {
            if (value < limit) out.append('0');
        }
        return out.append(value);
    }
}
