package io.logz.sawmill.processors;

import com.google.common.io.Resources;
import com.google.common.net.InetAddresses;
import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static io.logz.sawmill.processors.GeoIpProcessor.Property.ALL_PROPERTIES;
import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;

public class GeoIpProcessorTest {
    private static DatabaseReader databaseReader;

    static {
        loadDatabaseReader();
    }

    private static void loadDatabaseReader() {
        try (InputStream inputStream = new GZIPInputStream(Resources.getResource("GeoLite2-City.mmdb.gz").openStream())) {
            databaseReader = new DatabaseReader.Builder(inputStream).withCache(new CHMCache()).build();
        } catch (IOException e) {
            throw new RuntimeException("failed to load geoip database", e);
        }

    }

    @Test
    public void testValidIpWithSpecificProperties() {
        String ip = "187.162.70.166";
        String source = "ipString";

        Map<String,Object> config = new HashMap<>();
        config.put("sourceField", source);
        config.put("properties", Arrays.asList("ip", "country_name", "country_code2", "city_name"));
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
    public void testValidIp() throws IOException, GeoIp2Exception {
        String ip = "187.162.70.166";
        String source = "ipString";
        String target = "geoip";

        GeoIpProcessor geoIpProcessor = new GeoIpProcessor(source, target, ALL_PROPERTIES, Arrays.asList("geoip"));

        Doc doc = createDoc(source, ip);
        assertThat(geoIpProcessor.process(doc).isSucceeded()).isTrue();
        assertThat(doc.hasField(target)).isTrue();

        Map<String, Object> geoIp = doc.getField(target);
        CityResponse response = databaseReader.city(InetAddresses.forString(ip));
        GeoIpProcessor.Property.ALL_PROPERTIES.stream().forEach(property -> {
            assertThat(geoIp.get(property.toString())).isEqualTo(property.getValue(response));
        });
    }

    @Test
    public void testNotFoundIp() {
        String ip = "0.0.0.0";
        String source = "ipString";
        String target = "geoip";

        GeoIpProcessor geoIpProcessor = new GeoIpProcessor(source, target, ALL_PROPERTIES, Arrays.asList("geoip"));

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

        GeoIpProcessor geoIpProcessor = new GeoIpProcessor(source, target, ALL_PROPERTIES, Arrays.asList("geoip"));

        Doc doc = createDoc(source, ip);

        assertThat(geoIpProcessor.process(doc).isSucceeded()).isTrue();
        assertThat(doc.hasField(target)).isFalse();
    }

    @Test
    public void testEmptyPropertiesIp() {
        String ip = "199.188.236.64";
        String source = "ipString";
        String target = "geoip";

        GeoIpProcessor geoIpProcessor = new GeoIpProcessor(source, target, ALL_PROPERTIES, Arrays.asList("geoip"));

        Doc doc = createDoc(source, ip);

        assertThat(geoIpProcessor.process(doc).isSucceeded()).isTrue();
        assertThat(doc.hasField(target)).isFalse();
    }
}
