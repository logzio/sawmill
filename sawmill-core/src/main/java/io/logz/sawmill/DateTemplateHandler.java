package io.logz.sawmill;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Function;

public class DateTemplateHandler {

    // keep compatibility until we change configs
    public Function<String, String> date() {
        return this::getCurrentDateByFormat;
    }

    public Function<String, String> dateTemplate() {
        return this::getCurrentDateByFormat;
    }

    private String getCurrentDateByFormat(String dateFormat) {
        return DateTimeFormatter.ofPattern(dateFormat).format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(System.currentTimeMillis()), ZoneOffset.UTC));
    }
}
