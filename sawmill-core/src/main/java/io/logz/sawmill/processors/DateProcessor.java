package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;

@ProcessorProvider(type = "date", factory = DateProcessor.Factory.class)
public class DateProcessor implements Processor {
    public static final DateTimeFormatter elasticPrintFormat = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z");
    private static ConcurrentMap<String, DateTimeFormatter> dateTimePatternToFormatter = new ConcurrentHashMap<>();

    private final String field;
    private final String targetField;
    private final List<String> formats;
    private final List<DateTimeFormatter> formatters;
    private final ZoneId timeZone;

    public DateProcessor(String field, String targetField, List<String> formats, ZoneId timeZone) {
        this.formatters = new ArrayList<>();
        this.field = field;
        this.targetField = targetField;
        this.formats = formats;
        this.timeZone = timeZone;

        formats.forEach(format -> {
            if (format.toUpperCase().startsWith("UNIX")) return;
            try {
                dateTimePatternToFormatter.computeIfAbsent(format, k -> DateTimeFormatter.ofPattern(format));
            } catch (IllegalArgumentException e) {
                throw new RuntimeException(String.format("failed to create date processor, format [%s] is not valid", format), e);
            }
            formatters.add(dateTimePatternToFormatter.get(format));
        });
    }

    @Override
    public ProcessResult process(Doc doc) {
        if (!doc.hasField(field)) {
            return ProcessResult.failure(String.format("failed to process date, field in path [%s] is missing", field));
        }

        Object value = doc.getField(field);
        ZonedDateTime dateTime = null;

        if (value instanceof Long) {
            long unixTimestamp = (Long) value;
            Instant instant;
            if (formats.contains("UNIX_MS")) {
                instant = Instant.ofEpochMilli(unixTimestamp);
            } else {
                instant = Instant.ofEpochSecond(unixTimestamp);
            }

            dateTime = ZonedDateTime.ofInstant(instant, timeZone);
        } else if (value instanceof String) {
            for (DateTimeFormatter formatter : formatters) {
                try {
                    dateTime = ZonedDateTime.parse(value.toString(), formatter.withZone(timeZone));
                    break;
                } catch (DateTimeParseException e) {
                    // keep trying
                }
            }
        }

        if (dateTime == null) {
            return ProcessResult.failure(String.format("failed to parse date in path [%s], [%s] is not one of the formats [%s]", field, value, formats));
        }

        doc.addField(targetField, dateTime.format(elasticPrintFormat));

        return ProcessResult.success();
    }

    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public Processor create(Map<String,Object> config) {
            DateProcessor.Configuration dateConfig = JsonUtils.fromJsonMap(Configuration.class, config);

            return new DateProcessor(dateConfig.getField(),
                    dateConfig.getTargetField() != null ? dateConfig.getTargetField() : "@timestamp",
                    dateConfig.getFormats(),
                    ZoneId.of(dateConfig.getTimeZone()));
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String field;
        private String targetField;

        /**
         * The format of the date string.
         * The format in this String is documented in https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html.
         * Example:
         *  "yyyy MM dd"
         */
        private List<String> formats;

        /**
         * The time zone
         * The zone in this String is documented in https://docs.oracle.com/javase/8/docs/api/java/time/ZoneId.html
         * Example:
         *  "UTC"
         */
        private String timeZone;

        public Configuration() { }

        public String getField() { return field; }

        public String getTargetField() {
            return targetField;
        }

        public List<String> getFormats() {
            return formats;
        }

        public String getTimeZone() {
            return timeZone;
        }
    }
}
