package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorParseException;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

@ProcessorProvider(type = "date", factory = DateProcessor.Factory.class)
public class DateProcessor implements Processor {
    public static final DateTimeFormatter elasticPrintFormat = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE)
            .appendLiteral('T')
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)
            .optionalStart()
            .appendFraction(NANO_OF_SECOND, 3, 3, true)
            .optionalStart()
            .appendOffsetId()
            .optionalEnd()
            .toFormatter()
            .withChronology(IsoChronology.INSTANCE)
            .withResolverStyle(ResolverStyle.STRICT);
    /**
     * Joda ISODateTimeFormat dateParser description
     * The motivation is supporting logstash ISO8601 pattern
     * generic ISO date parser for parsing dates with a possible zone.
     * <p>
     * It accepts formats described by the following syntax:
     * <pre>
     * date              = date-element ['T' offset]
     * date-element      = std-date-element | ord-date-element | week-date-element
     * std-date-element  = yyyy ['-' MM ['-' dd]]
     * ord-date-element  = yyyy ['-' DDD]
     * week-date-element = xxxx '-W' ww ['-' e]
     * offset            = 'Z' | (('+' | '-') HH [':' mm [':' ss [('.' | ',') SSS]]])
     * </pre>
     */
    public static final DateTimeFormatter iso8601 = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE)
            .appendLiteral('T')
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .optionalStart()
                .appendLiteral(':')
                .appendValue(SECOND_OF_MINUTE, 2)
                .optionalStart()
                    .optionalStart()
                        .appendLiteral(',')
                    .optionalEnd()
                    .optionalStart()
                        .appendLiteral('.')
                    .optionalEnd()
                .appendFraction(NANO_OF_SECOND, 0, 9, false)
                    .optionalStart()
                        .appendOffsetId()
                    .optionalEnd()
                .optionalEnd()
            .optionalEnd()
            .toFormatter()
            .withChronology(IsoChronology.INSTANCE)
            .withResolverStyle(ResolverStyle.STRICT);
    private static Map<String, DateTimeFormatter> dateTimePatternToFormatter = new ConcurrentHashMap<>();

    static {
        dateTimePatternToFormatter.put("ISO8601", iso8601);
    }

    private final String field;
    private final String targetField;
    private final List<String> formats;
    private final List<DateTimeFormatter> formatters;
    private final ZoneId timeZone;

    public DateProcessor(String field, String targetField, List<String> formats, ZoneId timeZone) {
        checkState(CollectionUtils.isNotEmpty(formats), "formats cannot be empty");
        this.field = checkNotNull(field, "field cannot be null");
        this.targetField = checkNotNull(targetField, "target field cannot be null");
        this.formats = formats;
        this.timeZone = timeZone;

        this.formatters = new ArrayList<>();

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

        Object dateTimeDocValue = doc.getField(field);

        ZonedDateTime dateTime = null;
        if (dateTimeDocValue instanceof Long) {
            dateTime = getUnixDateTime((Long) dateTimeDocValue);
        } else if (dateTimeDocValue instanceof String) {
            dateTime = getISODateTime((String) dateTimeDocValue);
        }

        if (dateTime == null) {
            return ProcessResult.failure(String.format("failed to parse date in path [%s], [%s] is not one of the formats [%s]", field, dateTimeDocValue, formats));
        }

        doc.addField(targetField, dateTime.format(elasticPrintFormat));

        return ProcessResult.success();
    }

    private ZonedDateTime getISODateTime(String value) {
        for (DateTimeFormatter formatter : formatters) {
            try {
                return getZonedDateTime(value, formatter);
            } catch (DateTimeParseException e) {
                // keep trying
            }
        }
        return null;
    }

    private ZonedDateTime getZonedDateTime(String value, DateTimeFormatter formatter) {
        TemporalAccessor temporal = formatter.parseBest(value, ZonedDateTime::from, LocalDateTime::from);
        if (temporal instanceof LocalDateTime) {
            return ZonedDateTime.of((LocalDateTime) temporal, ZoneOffset.UTC);
        } else {
            return ZonedDateTime.from(temporal);
        }
    }

    private ZonedDateTime getUnixDateTime(Long value) {
        long unixTimestamp = value;
        Instant instant;
        if (formats.contains("UNIX_MS")) {
            instant = Instant.ofEpochMilli(unixTimestamp);
        } else {
            instant = Instant.ofEpochSecond(unixTimestamp);
        }

        if (timeZone == null) {
            return ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"));
        } else {
            return ZonedDateTime.ofInstant(instant, timeZone);
        }
    }

    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public Processor create(Map<String,Object> config) {
            DateProcessor.Configuration dateConfig = JsonUtils.fromJsonMap(Configuration.class, config);

            if (CollectionUtils.isEmpty(dateConfig.getFormats())) {
                throw new ProcessorParseException("cannot create date processor without any format");
            }

            String field = dateConfig.getField();
            String targetField = dateConfig.getTargetField();
            List<String> formats = dateConfig.getFormats();
            String timeZone = dateConfig.getTimeZone();
            ZoneId zoneId = timeZone != null ? ZoneId.of(timeZone) : null;

            return new DateProcessor(field, targetField, formats, zoneId);
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String field;
        private String targetField = "@timestamp";

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
