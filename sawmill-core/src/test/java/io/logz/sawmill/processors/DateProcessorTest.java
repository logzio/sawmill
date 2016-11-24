package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;

public class DateProcessorTest {
    @Test
    public void testUnixMsFormat() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("UTC");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        Doc doc = createDoc(field, zonedDateTime.toInstant().toEpochMilli());

        DateProcessor dateProcessor = new DateProcessor(field, targetField, Arrays.asList("UNIX_MS"), zoneId);

        assertThat(dateProcessor.process(doc).isSucceeded()).isTrue();
        assertThat((ZonedDateTime) doc.getField(targetField)).isEqualTo(zonedDateTime);
    }

    @Test
    public void testUnixFormat() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("UTC");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId).truncatedTo(ChronoUnit.SECONDS);
        Doc doc = createDoc(field, zonedDateTime.toInstant().toEpochMilli() / 1000);

        DateProcessor dateProcessor = new DateProcessor(field, targetField, Arrays.asList("UNIX"), zoneId);

        assertThat(dateProcessor.process(doc).isSucceeded()).isTrue();
        assertThat((ZonedDateTime) doc.getField(targetField)).isEqualTo(zonedDateTime);
    }

    @Test
    public void testSeveralISOPatterns() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("UTC");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        List<String> formats = Arrays.asList("dd/MM/yyyy HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ssZ", "yyyy-MM-dd HH:mm:ss.SSSSS", "ddMMyyyy HHmmssSSS");

        DateProcessor dateProcessor = new DateProcessor(field, targetField, formats, zoneId);

        formats.forEach(format -> {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
            String dateString = zonedDateTime.format(formatter);
            Doc doc = createDoc(field, dateString);

            ZonedDateTime expectedDateTime = LocalDateTime.parse(dateString, formatter).atZone(zoneId);

            assertThat(dateProcessor.process(doc).isSucceeded()).isTrue();
            assertThat((ZonedDateTime) doc.getField(targetField)).isEqualTo(expectedDateTime);
        });
    }
}
