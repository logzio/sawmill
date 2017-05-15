package io.logz.sawmill;

import com.github.mustachejava.TemplateFunction;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class DateTemplateHandler {

    public TemplateFunction dateTemplate() {
        return this::getCurrentDateByFormat;
    }

    private String getCurrentDateByFormat(String dateFormat) {
        return DateTimeFormatter.ofPattern(dateFormat).format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(System.currentTimeMillis()), ZoneOffset.UTC));
    }
}
