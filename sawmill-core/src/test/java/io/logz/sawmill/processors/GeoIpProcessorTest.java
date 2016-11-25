package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import org.junit.Test;

import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;

public class GeoIpProcessorTest {
    @Test
    public void testValidIp() {
        String ip = "187.162.70.166";
        String source = "ipString";
        String target = "geoip";

        GeoIpProcessor geoIpProcessor = new GeoIpProcessor(source, target);

        Doc doc = createDoc(source, ip);

        assertThat(geoIpProcessor.process(doc).isSucceeded()).isTrue();
        assertThat(doc.hasField(target)).isTrue();
        Map<String, Object> geoIp = doc.getField(target);
        assertThat(geoIp.get("country_name")).isEqualTo("Mexico");
        assertThat(geoIp.get("city_name")).isEqualTo("Mexico City");
    }

    @Test
    public void testNotFoundIp() {
        String ip = "0.0.0.0";
        String source = "ipString";
        String target = "geoip";

        GeoIpProcessor geoIpProcessor = new GeoIpProcessor(source, target);

        Doc doc = createDoc(source, ip);

        assertThat(geoIpProcessor.process(doc).isSucceeded()).isTrue();
        assertThat(doc.hasField(target)).isFalse();
    }
}
