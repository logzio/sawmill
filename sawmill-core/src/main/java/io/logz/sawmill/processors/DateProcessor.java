package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
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
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.util.Objects.requireNonNull;

@ProcessorProvider(type = "date", factory = DateProcessor.Factory.class)
public class DateProcessor implements Processor {
    public static final DateTimeFormatter ELASTIC = new DateTimeFormatterBuilder()
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
    public static final DateTimeFormatter ISO8601 = new DateTimeFormatterBuilder()
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

    public static final DateTimeFormatter UNIX = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.INSTANT_SECONDS, 1, 10, SignStyle.NEVER)
            .toFormatter();

    public static final DateTimeFormatter UNIX_MS = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.INSTANT_SECONDS, 1, 10, SignStyle.NEVER)
            .appendValue(ChronoField.MILLI_OF_SECOND, 3)
            .toFormatter();

    private static Map<String, DateTimeFormatter> dateTimePatternToFormatter = new ConcurrentHashMap<>();

    static {
        dateTimePatternToFormatter.put("ISO8601", ISO8601);
        dateTimePatternToFormatter.put("UNIX", UNIX);
        dateTimePatternToFormatter.put("UNIX_MS", UNIX_MS);
        dateTimePatternToFormatter.put("ELASTIC", ELASTIC);
    }

    private final String field;
    private final String targetField;
    private final List<String> formats;
    // per format: the parsed value, or null
    private final List<Function<String, ZonedDateTime>> parsers;
    private final ZoneId timeZone;
    // the zone of parsed values that carry none
    private final ZoneId zoneIfMissing;
    private final Function<ZonedDateTime, String> output;

    public DateProcessor(String field, String targetField, List<String> formats, ZoneId timeZone, String outputFormat) {
        checkState(CollectionUtils.isNotEmpty(formats), "formats cannot be empty");
        this.field = requireNonNull(field, "field cannot be null");
        this.targetField = requireNonNull(targetField, "target field cannot be null");
        this.formats = formats;
        this.timeZone = timeZone;
        this.zoneIfMissing = timeZone == null ? ZoneOffset.UTC : timeZone;
        DateTimeFormatter outputFormatter = computeAndGetFormatter(outputFormat, timeZone);
        this.output = "ELASTIC".equals(outputFormat) ? ElasticDateFormat::format : outputFormatter::format;

        this.parsers = new ArrayList<>();
        formats.forEach(format -> {
            DateTimeFormatter formatter = computeAndGetFormatter(format, timeZone);
            FastDateParser.Parser fastParser = FastDateParser.forFormat(format);

            parsers.add(value -> {
                ZonedDateTime dateTime = fastParser == null ? null : fastParser.parse(value, zoneIfMissing);
                return dateTime != null ? dateTime : getZonedDateTime(value, formatter);
            });
        });

    }

    private DateTimeFormatter computeAndGetFormatter(String format, ZoneId timeZone) {
        if (format.toUpperCase().startsWith("UNIX")) {
            return dateTimePatternToFormatter.get(format).withZone(timeZone == null ? ZoneId.of("UTC") : timeZone);
        }

        try {
            return dateTimePatternToFormatter.computeIfAbsent(format, k -> new DateTimeFormatterBuilder().parseCaseInsensitive().appendPattern(format).toFormatter());
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(String.format("failed to create date processor, format [%s] is not valid", format), e);
        }
    }

    @Override
    public ProcessResult process(Doc doc) {
        if (!doc.hasField(field)) {
            return ProcessResult.failure(() -> String.format("failed to process date, field in path [%s] is missing", field));
        }

        Object dateTimeDocValue = doc.getField(field);

        ZonedDateTime dateTime = null;
        if (dateTimeDocValue instanceof Number && formats.contains("UNIX")) {
            long epocTimeInMilis = ((Number) (((Number) dateTimeDocValue).doubleValue() * 1000)).longValue();
            dateTime = getEpochMillisDateTime(epocTimeInMilis);

        } else if (dateTimeDocValue instanceof Number && formats.contains("UNIX_MS")) {
            long epocTimeInMilis = ((Number) dateTimeDocValue).longValue();
            dateTime = getEpochMillisDateTime(epocTimeInMilis);

        } else if (dateTimeDocValue instanceof String) {
            dateTime = getISODateTime((String) dateTimeDocValue);
        }

        if (dateTime == null) {
            // render the value now: it belongs to the doc and may change before the message is read
            String shownValue = String.valueOf(dateTimeDocValue);
            return ProcessResult.failure(() -> String.format("failed to parse date in path [%s], [%s] is not one of the formats [%s]", field, shownValue, formats));
        }

        doc.addField(targetField, output.apply(dateTime));

        return ProcessResult.success();
    }

    private ZonedDateTime getISODateTime(String value) {
        for (Function<String, ZonedDateTime> parser : parsers) {
            ZonedDateTime dateTime = parser.apply(value);
            if (dateTime != null) return dateTime;
        }
        return null;
    }

    /**
     * Same result as {@code formatter.parseBest(value, ZonedDateTime::from, LocalDateTime::from)}, or {@code null} where
     * parseBest throws, without {@code ZonedDateTime.from} throwing (and filling in a stack trace) for every value that
     * has no zone: the zone is checked first.
     */
    private ZonedDateTime getZonedDateTime(String value, DateTimeFormatter formatter) {
        try {
            TemporalAccessor parsed = formatter.parse(value);
            try {
                // the zone query itself throws for an offset outside +-18:00
                if (parsed.query(TemporalQueries.zone()) != null) return ZonedDateTime.from(parsed);
            } catch (RuntimeException e) {
                // parseBest moves on to LocalDateTime::from
            }
            return ZonedDateTime.of(LocalDateTime.from(parsed), zoneIfMissing);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private ZonedDateTime getEpochMillisDateTime(Long unixTimestamp) {
        Instant instant = Instant.ofEpochMilli(unixTimestamp);
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
                throw new ProcessorConfigurationException("cannot create date processor without any format");
            }

            String field = dateConfig.getField();
            String targetField = dateConfig.getTargetField();
            List<String> formats = dateConfig.getFormats();
            String timeZone = dateConfig.getTimeZone();
            ZoneId zoneId = timeZone != null ? ZoneId.of(timeZone) : null;
            String outputFormat = dateConfig.getOutputFormat();

            return new DateProcessor(field, targetField, formats, zoneId, outputFormat);
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

        private String outputFormat = "ELASTIC";

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

        public String getOutputFormat() {
            return outputFormat;
        }
    }
}
