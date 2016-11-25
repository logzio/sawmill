package io.logz.sawmill.processors;

import com.google.common.io.Resources;
import com.google.common.net.InetAddresses;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Continent;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Location;
import com.maxmind.geoip2.record.Postal;
import com.maxmind.geoip2.record.Subdivision;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class GeoIpProcessor implements Processor {
    private static final String TYPE = "geoIp";

    private static DatabaseReader databaseReader;

    static {
        loadDatabaseReader();
    }

    private static void loadDatabaseReader() {
        try {
            InputStream inputStream = new GZIPInputStream(Resources.getResource("GeoLite2-City.mmdb.gz").openStream());
            databaseReader = new DatabaseReader.Builder(inputStream).build();

        } catch (IOException e) {
            throw new RuntimeException("failed to load geoip database", e);
        }

    }

    private final String source;
    private final String target;

    public GeoIpProcessor(String source, String target) {
        this.source = source;
        this.target = target;
    }

    @Override
    public String getType() { return TYPE; }

    @Override
    public ProcessResult process(Doc doc) {
        if (!doc.hasField(source)) {
            return new ProcessResult(false, String.format("failed to get ip from [%s], field missing", source));
        }

        String ip = doc.getField(source);
        InetAddress ipAddress = InetAddresses.forString(ip);

        Map<String, Object> geoIp;

        try {
            geoIp = extractGeoIp(ipAddress);
        } catch (AddressNotFoundException e) {
            geoIp = null;
        } catch (Exception e) {
            return new ProcessResult(false, String.format("failed to fetch geoIp for [%s], errorMsg [%s]", ip, e.getMessage()));
        }

        if (geoIp != null) {
            doc.addField(target, geoIp);
        }

        return new ProcessResult(true);
    }

    private Map<String, Object> extractGeoIp(InetAddress ipAddress) throws GeoIp2Exception, IOException {
        CityResponse response = databaseReader.city(ipAddress);

        Map<String, Object> geoIp = new HashMap<>();
        Country country = response.getCountry();
        geoIp.put("country_name", country.getName());
        geoIp.put("country_iso_code", country.getIsoCode());

        Continent continent = response.getContinent();
        geoIp.put("continent_code", continent.getCode());

        Subdivision subdivision = response.getMostSpecificSubdivision();
        geoIp.put("region_name", subdivision.getIsoCode());
        geoIp.put("real_region_name", subdivision.getName());

        City city = response.getCity();
        geoIp.put("city_name", city.getName());

        Location location = response.getLocation();
        geoIp.put("latitude", location.getLatitude());
        geoIp.put("longitude", location.getLongitude());
        geoIp.put("timezone", location.getTimeZone());

        Postal postal = response.getPostal();
        geoIp.put("postal_code", postal.getCode());

        geoIp.put("ip", ipAddress.getHostAddress());

        return geoIp;
    }

    @ProcessorProvider(name = TYPE)
    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public Processor create(String config) {
            GeoIpProcessor.Configuration geoIpConfig = JsonUtils.fromJsonString(GeoIpProcessor.Configuration.class, config);

            return new GeoIpProcessor(geoIpConfig.getSource(),
                    geoIpConfig.getTarget() != null ? geoIpConfig.getTarget() : "geoip");
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String source;
        private String target;

        public Configuration() { }

        public Configuration(String source, String target) {
            this.source = source;
            this.target = target;
        }

        public String getSource() { return source; }

        public String getTarget() { return target; }
    }
}
