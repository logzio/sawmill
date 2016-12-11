package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static io.logz.sawmill.processors.GeoIpProcessor.Property.ALL_PROPERTIES;
import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;

public class GeoIpProcessorTest {
    @Test
    public void testValidIpWithSpecificProperties() {
        String ip = "187.162.70.166";
        String source = "ipString";


        Map<String,Object> config = new HashMap<>();
        config.put("sourceField", source);
        config.put("properties", Arrays.asList("ip", "country_name", "country_iso_code", "city_name"));
        GeoIpProcessor geoIpProcessor = new GeoIpProcessor.Factory().create(config);

        Doc doc = createDoc(source, ip);

        ProcessResult processResult = geoIpProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat(doc.hasField("geoip")).isTrue();
        Map<String, Object> geoIp = doc.getField("geoip");
        assertThat(geoIp.size()).isEqualTo(4);
        assertThat(geoIp.get("country_name")).isEqualTo("Mexico");
        assertThat(geoIp.get("city_name")).isEqualTo("Mexico City");
        assertThat(geoIp.get("ip")).isEqualTo(ip);
    }

    @Test
    public void testValidIp() {
        String ip = "187.162.70.166";
        String source = "ipString";
        String target = "geoip";

        GeoIpProcessor geoIpProcessor = new GeoIpProcessor(source, target, ALL_PROPERTIES);

        Doc doc = createDoc(source, ip);

        assertThat(geoIpProcessor.process(doc).isSucceeded()).isTrue();
        assertThat(doc.hasField(target)).isTrue();
        Map<String, Object> geoIp = doc.getField(target);
        assertThat(geoIp.get("country_name")).isEqualTo("Mexico");
        assertThat(geoIp.get("city_name")).isEqualTo("Mexico City");
        assertThat(geoIp.get("ip")).isEqualTo(ip);
    }

    @Test
    public void testNotFoundIp() {
        String ip = "0.0.0.0";
        String source = "ipString";
        String target = "geoip";

        GeoIpProcessor geoIpProcessor = new GeoIpProcessor(source, target, ALL_PROPERTIES);

        Doc doc = createDoc(source, ip);

        assertThat(geoIpProcessor.process(doc).isSucceeded()).isTrue();
        assertThat(doc.hasField(target)).isFalse();
    }
}
