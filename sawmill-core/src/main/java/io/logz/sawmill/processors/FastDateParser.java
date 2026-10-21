package io.logz.sawmill.processors;

import java.time.LocalDateTime;
import java.time.Month;
import java.time.Year;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * Direct parsers for the most common date shapes, used by {@link DateProcessor} before java.time.
 *
 * Each parser handles only input whose result is certain: the exact canonical shape (ASCII digits, literal
 * separators) with every field inside its always-valid range. For anything else, including values java.time would
 * adjust (24:00, day 30 in February) or reject, it returns {@code null} and the caller parses with the formatter as
 * before. The result is built with the same calls java.time resolution ends in, so it is the same value.
 */
final class FastDateParser {

    private FastDateParser() {
    }

    /** Parser for {@code format}, or {@code null} if it has none. */
    static Parser forFormat(String format) {
        switch (format) {
            case "yyyy-MM-dd HH:mm:ss.SSS":
                return FastDateParser::spaceSeparatedMillis;
            case "ISO8601":
                return FastDateParser::iso8601;
            default:
                return null;
        }
    }

    interface Parser {
        /** The parsed value, or {@code null} if the input is not one this parser handles. */
        ZonedDateTime parse(String value, ZoneId zoneIfMissing);
    }

    /** {@code yyyy-MM-dd HH:mm:ss.SSS} (pattern letters, SMART resolution) */
    static ZonedDateTime spaceSeparatedMillis(String value, ZoneId zoneIfMissing) {
        if (value.length() != 23 || value.charAt(10) != ' ' || value.charAt(19) != '.') return null;
        LocalDateTime dateTime = dateAndTime(value);
        if (dateTime == null) return null;

        int millis = number(value, 20, 3);
        if (millis < 0) return null;
        return ZonedDateTime.of(dateTime.withNano(millis * 1_000_000), zoneIfMissing);
    }

    /** {@code yyyy-MM-ddTHH:mm:ss}, optional {@code .} and 1-9 fraction digits, optional {@code Z} or {@code +HH:MM}. */
    static ZonedDateTime iso8601(String value, ZoneId zoneIfMissing) {
        int length = value.length();
        if (length < 19 || value.charAt(10) != 'T') return null;
        LocalDateTime dateTime = dateAndTime(value);
        if (dateTime == null) return null;

        int i = 19;
        if (i < length && value.charAt(i) == '.') {
            int start = ++i;
            int nanos = 0;
            while (i < length && i - start < 9 && isDigit(value.charAt(i))) {
                nanos = nanos * 10 + (value.charAt(i) - '0');
                i++;
            }
            int digits = i - start;
            if (digits == 0) return null;
            for (int d = digits; d < 9; d++) nanos *= 10;
            dateTime = dateTime.withNano(nanos);
        }

        if (i == length) return ZonedDateTime.of(dateTime, zoneIfMissing);
        if (value.charAt(i) == 'Z' && i + 1 == length) return ZonedDateTime.of(dateTime, ZoneOffset.UTC);

        char sign = value.charAt(i);
        if ((sign != '+' && sign != '-') || length - i != 6 || value.charAt(i + 3) != ':') return null;
        int hours = number(value, i + 1, 2);
        int minutes = number(value, i + 4, 2);
        if (hours < 0 || minutes < 0 || hours > 17 || minutes > 59) return null;
        int seconds = (hours * 3600 + minutes * 60) * (sign == '-' ? -1 : 1);
        return ZonedDateTime.of(dateTime, ZoneOffset.ofTotalSeconds(seconds));
    }

    /** {@code yyyy-MM-dd?HH:mm:ss} at the start of {@code value} (the caller checks position 10), or null. */
    private static LocalDateTime dateAndTime(String value) {
        if (value.charAt(4) != '-' || value.charAt(7) != '-' || value.charAt(13) != ':' || value.charAt(16) != ':') {
            return null;
        }
        int year = number(value, 0, 4);
        int month = number(value, 5, 2);
        int day = number(value, 8, 2);
        int hour = number(value, 11, 2);
        int minute = number(value, 14, 2);
        int second = number(value, 17, 2);
        if (year < 1 || month < 1 || month > 12 || day < 1 || hour < 0 || hour > 23 || minute < 0 || minute > 59
                || second < 0 || second > 59) {
            return null;
        }
        if (day > Month.of(month).length(Year.isLeap(year))) return null;
        return LocalDateTime.of(year, month, day, hour, minute, second);
    }

    /** Non-negative value of {@code digits} ASCII digits at {@code start}, or -1. */
    private static int number(String value, int start, int digits) {
        int result = 0;
        for (int i = start; i < start + digits; i++) {
            char c = value.charAt(i);
            if (!isDigit(c)) return -1;
            result = result * 10 + (c - '0');
        }
        return result;
    }

    private static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }
}
