package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DateProcessor implements Processor {
    private static final String TYPE = "date";

    private static Map<String, DateTimeFormatter> dateTimePatternToFormatter;

    static {
        dateTimePatternToFormatter = new HashMap<>();
        dateTimePatternToFormatter.put("UNIX", new DateTimeFormatterBuilder()
                .appendValue(ChronoField.INSTANT_SECONDS, 1, 19, SignStyle.NEVER)
                .toFormatter());
        dateTimePatternToFormatter.put("UNIX_MS", new DateTimeFormatterBuilder()
                .appendValue(ChronoField.INSTANT_SECONDS, 1, 19, SignStyle.NEVER)
                .appendValue(ChronoField.MILLI_OF_SECOND, 3)
                .toFormatter());
    }

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
            if (!dateTimePatternToFormatter.containsKey(format)) {
                dateTimePatternToFormatter.putIfAbsent(format, DateTimeFormatter.ofPattern(format));
            }

            formatters.add(dateTimePatternToFormatter.get(format));
        });
    }

    @Override
    public String getType() { return TYPE; }

    @Override
    public ProcessResult process(Doc doc) {
        Object value = doc.getField(this.field);
        ZonedDateTime dateTime = null;
        for (DateTimeFormatter formatter : formatters) {
            try {
                dateTime = LocalDateTime.parse(value.toString(), formatter).atZone(timeZone);
            } catch (DateTimeParseException e) {
                // keep trying
            }
        }

        if (dateTime == null) {
            return new ProcessResult(false,
                    String.format("failed to parse date in path [%s], [%s] is not one of the formats [%s]", field, value, formats));
        }

        doc.addField(targetField, dateTime);

        return new ProcessResult(true);
    }

    @ProcessorProvider(name = TYPE)
    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public Processor create(String config) {
            DateProcessor.Configuration dateConfig = JsonUtils.fromJsonString(DateProcessor.Configuration.class, config);

            return new DateProcessor(dateConfig.getField(),
                    dateConfig.getTargetField() != null ? dateConfig.getTargetField() : "@timestamp",
                    dateConfig.getFormats(),
                    ZoneId.of(dateConfig.getTimeZone()));
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String field;
        private String targetField;
        private List<String> formats;
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
