package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;

public class GeoIpProcessorTest {
    @Test
    public void testValidIpWithSpecificProperties() {
        String ip = "187.162.70.166";
        String source = "ipString";


        Map<String,Object> config = createConfig("sourceField", source,
                "properties", Arrays.asList("ip", "country_name", "country_code2", "city_name"));
        GeoIpProcessor geoIpProcessor = createProcessor(GeoIpProcessor.class, config);

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
        String target = "{{geoipField}}";

        Map<String, Object> config = createConfig("sourceField", source,
                "targetField", target,
                "tagsOnSuccess", Arrays.asList("geoip"));

        GeoIpProcessor geoIpProcessor = createProcessor(GeoIpProcessor.class, config);

        Doc doc = createDoc(source, ip, "geoipField", "geo");

        assertThat(geoIpProcessor.process(doc).isSucceeded()).isTrue();
        assertThat(doc.hasField("geo")).isTrue();
        Map<String, Object> geoIp = doc.getField("geo");
        assertThat(geoIp.get("country_name")).isEqualTo("Mexico");
        assertThat(geoIp.get("country_code2")).isEqualTo("MX");
        assertThat(geoIp.get("continent_code")).isEqualTo("NA");
        assertThat(geoIp.get("region_name")).isEqualTo("CMX");
        assertThat(geoIp.get("real_region_name")).isEqualTo("Mexico City");
        assertThat(geoIp.get("city_name")).isEqualTo("Mexico City");
        assertThat(geoIp.get("ip")).isEqualTo(ip);
        assertThat(geoIp.get("timezone")).isEqualTo("America/Mexico_City");
        assertThat(geoIp.get("postal_code")).isEqualTo("63009");
        assertThat(geoIp.get("longitude")).isEqualTo(-99.1439d);
        assertThat(geoIp.get("latitude")).isEqualTo(19.4357d);
        assertThat(geoIp.get("location")).isEqualTo(Arrays.asList(-99.1439d, 19.4357d));
        assertThat(((List)doc.getField("tags")).contains("geoip")).isTrue();
    }

    @Test
    public void testNotFoundIp() {
        String ip = "0.0.0.0";
        String source = "ipString";
        String target = "geoip";

        Map<String, Object> config = createConfig("sourceField", source,
                "tagsOnSuccess", Arrays.asList("geoip"));

        GeoIpProcessor geoIpProcessor = createProcessor(GeoIpProcessor.class, config);

        Doc doc = createDoc(source, ip);

        assertThat(geoIpProcessor.process(doc).isSucceeded()).isTrue();
        assertThat(doc.hasField(target)).isFalse();
        assertThat(doc.hasField("tags")).isFalse();
    }

    @Test
    public void testInternalIp() {
        String ip = "192.168.1.1";
        String source = "ipString";
        String target = "geoip";

        Map<String, Object> config = createConfig("sourceField", source,
                "tagsOnSuccess", Arrays.asList("geoip"));

        GeoIpProcessor geoIpProcessor = createProcessor(GeoIpProcessor.class, config);

        Doc doc = createDoc(source, ip);

        assertThat(geoIpProcessor.process(doc).isSucceeded()).isTrue();
        assertThat(doc.hasField(target)).isFalse();
    }

    @Test
    public void testEmptyPropertiesIp() {
        String ip = "199.188.236.64";
        String source = "ipString";
        String target = "geoip";

        Map<String, Object> config = createConfig("sourceField", source,
                "tagsOnSuccess", Arrays.asList("geoip"));

        GeoIpProcessor geoIpProcessor = createProcessor(GeoIpProcessor.class, config);

        Doc doc = createDoc(source, ip);

        assertThat(geoIpProcessor.process(doc).isSucceeded()).isTrue();
        assertThat(doc.hasField(target)).isFalse();
    }
}
