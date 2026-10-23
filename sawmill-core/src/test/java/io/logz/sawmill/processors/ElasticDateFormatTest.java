package io.logz.sawmill.processors;

import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

public class ElasticDateFormatTest {

    @Test
    public void testMatchesFormatter() {
        List<ZoneId> zones = new ArrayList<>();
        // sorted: the set's iteration order is unspecified, so the seed alone does not fix which zones are picked
        for (String id : new TreeSet<>(ZoneId.getAvailableZoneIds())) zones.add(ZoneId.of(id));
        Random random = new Random(13);

        for (int n = 0; n < 200000; n++) {
            ZoneId zone = random.nextInt(4) == 0
                    ? ZoneOffset.ofTotalSeconds(random.nextInt(2 * 18 * 3600 + 1) - 18 * 3600)
                    : zones.get(random.nextInt(zones.size()));
            LocalDateTime local = LocalDateTime.of(random.nextInt(10100) - 50, 1 + random.nextInt(12), 1 + random.nextInt(28),
                    random.nextInt(24), random.nextInt(60), random.nextInt(60), random.nextInt(1_000_000_000));
            ZonedDateTime dateTime = ZonedDateTime.of(local, zone);

            assertThat(ElasticDateFormat.format(dateTime)).as("%s", dateTime).isEqualTo(dateTime.format(DateProcessor.ELASTIC));
        }
    }

    @Test
    public void testEdges() {
        ZonedDateTime base = ZonedDateTime.of(2026, 10, 15, 9, 0, 0, 999_999_999, ZoneOffset.UTC);
        assertThat(ElasticDateFormat.format(base)).isEqualTo("2026-10-15T09:00:00.999Z");
        assertThat(ElasticDateFormat.format(base.withZoneSameLocal(ZoneOffset.ofHoursMinutesSeconds(-5, -30, -15))))
                .isEqualTo(base.withZoneSameLocal(ZoneOffset.ofHoursMinutesSeconds(-5, -30, -15)).format(DateProcessor.ELASTIC));
        assertThat(ElasticDateFormat.format(Instant.parse("1800-01-01T00:00:00Z").atZone(ZoneId.of("Europe/Amsterdam"))))
                .isEqualTo(Instant.parse("1800-01-01T00:00:00Z").atZone(ZoneId.of("Europe/Amsterdam")).format(DateProcessor.ELASTIC));
        assertThat(ElasticDateFormat.format(ZonedDateTime.of(0, 1, 1, 0, 0, 0, 0, ZoneOffset.MAX))).isEqualTo("0000-01-01T00:00:00.000+18:00");
        assertThat(ElasticDateFormat.format(ZonedDateTime.of(10000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC))).isEqualTo("+10000-01-01T00:00:00.000Z");
    }
}
