package io.logz.sawmill.processors;

import com.google.common.io.Resources;
import com.google.common.net.InetAddresses;
import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorExecutionException;
import io.logz.sawmill.utilities.JsonUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static com.google.common.base.Preconditions.checkNotNull;

@ProcessorProvider(type = "geoIp", factory = GeoIpProcessor.Factory.class)
public class GeoIpProcessor implements Processor {
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

    private final String sourceField;
    private final String targetField;
    private final List<Property> properties;

    public GeoIpProcessor(String sourceField, String targetField, List<Property> properties) {
        this.sourceField = checkNotNull(sourceField, "source field cannot be null");
        this.targetField = targetField;
        this.properties = properties;
    }

    @Override
    public ProcessResult process(Doc doc) {
        if (!doc.hasField(sourceField)) {
            return ProcessResult.failure(String.format("failed to get ip from [%s], field missing", sourceField));
        }

        String ip = doc.getField(sourceField);
        if (!InetAddresses.isInetAddress(ip)) {
            return ProcessResult.failure(String.format("failed to process geoIp, source field [%s] in path [%s] is not a valid IP string", ip, sourceField));
        }
        InetAddress ipAddress = InetAddresses.forString(ip);

        Map<String, Object> geoIp;

        try {
            geoIp = extractGeoIp(ipAddress);
        } catch (AddressNotFoundException e) {
            geoIp = null;
        } catch (Exception e) {
            return ProcessResult.failure(String.format("failed to fetch geoIp for [%s]", ip),
                    new ProcessorExecutionException("geoIp", e));
        }

        if (geoIp != null) {
            doc.addField(targetField, geoIp);
        }

        return ProcessResult.success();
    }

    private Map<String, Object> extractGeoIp(InetAddress ipAddress) throws GeoIp2Exception, IOException {
        CityResponse response = databaseReader.city(ipAddress);

        Map<String, Object> geoIp = new HashMap<>();
        for (Property property : properties) {
            Object propertyValue = property.getValue(response);

            if (propertyValue != null) {
                geoIp.put(property.toString(), propertyValue);
            }
        }

        return geoIp;
    }

    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public GeoIpProcessor create(Map<String,Object> config) {
            GeoIpProcessor.Configuration geoIpConfig = JsonUtils.fromJsonMap(Configuration.class, config);

            return new GeoIpProcessor(geoIpConfig.getSourceField(),
                    geoIpConfig.getTargetField() != null ? geoIpConfig.getTargetField() : "geoip",
                    geoIpConfig.getProperties() != null ? geoIpConfig.getProperties() : Property.ALL_PROPERTIES);
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String sourceField;
        private String targetField;
        private List<Property> properties;

        public Configuration() { }

        public Configuration(String sourceField, String targetField) {
            this.sourceField = sourceField;
            this.targetField = targetField;
        }

        public String getSourceField() { return sourceField; }

        public String getTargetField() { return targetField; }

        public List<Property> getProperties() {
            return properties;
        }
    }

    public enum Property {
        IP {
            @Override
            public String getValue(CityResponse response) {
                return response.getTraits().getIpAddress();
            }
        },
        COUNTRY_NAME {
            @Override
            public String getValue(CityResponse response) {
                return response.getCountry().getName();
            }
        },
        COUNTRY_CODE2 {
            @Override
            public String getValue(CityResponse response) {
                return response.getCountry().getIsoCode();
            }
        },
        CONTINENT_CODE {
            @Override
            public String getValue(CityResponse response) {
                return response.getContinent().getCode();
            }
        },
        REGION_NAME {
            @Override
            public String getValue(CityResponse response) {
                return response.getLeastSpecificSubdivision().getIsoCode();
            }
        },
        REAL_REGION_NAME {
            @Override
            public String getValue(CityResponse response) {
                return response.getLeastSpecificSubdivision().getName();
            }
        },
        CITY_NAME {
            @Override
            public String getValue(CityResponse response) {
                return response.getCity().getName();
            }
        },
        LATITUDE {
            @Override
            public Double getValue(CityResponse response) {
                return response.getLocation().getLatitude();
            }
        },
        LONGITUDE {
            @Override
            public Object getValue(CityResponse response) {
                return response.getLocation().getLongitude();
            }
        },
        TIMEZONE {
            @Override
            public Object getValue(CityResponse response) {
                return response.getLocation().getTimeZone();
            }
        },
        POSTAL_CODE {
            @Override
            public Object getValue(CityResponse response) {
                return response.getPostal().getCode();
            }
        },
        LOCATION {
            @Override
            public Object getValue(CityResponse response) {
                return Arrays.asList(response.getLocation().getLongitude(),response.getLocation().getLatitude());
            }
        },
        DMA_CODE {
            @Override
            public Object getValue(CityResponse response) {
                return response.getLocation().getMetroCode();
            }
        };

        public static List<Property> ALL_PROPERTIES = new ArrayList<>(EnumSet.allOf(Property.class));

        @Override
        public String toString() {
            return this.name().toLowerCase();
        }

        public abstract Object getValue(CityResponse response);
    }
}
