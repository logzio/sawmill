package io.logz.sawmill.processors;

import com.google.common.io.Resources;
import com.google.common.net.InetAddresses;
import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.samskivert.mustache.Template;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.TemplateService;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorExecutionException;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;

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
import static com.google.common.base.Preconditions.checkState;
import static io.logz.sawmill.processors.GeoIpProcessor.Property.ALL_PROPERTIES;
import static io.logz.sawmill.processors.GeoIpProcessor.Property.LOCATION;
import static java.util.Collections.EMPTY_LIST;

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

    private final Template sourceField;
    private final Template targetField;
    private final List<Property> properties;
    private final List<String> tagsOnSuccess;

    public GeoIpProcessor(Template sourceField, Template targetField, List<Property> properties, List<String> tagsOnSuccess) {
        checkState(CollectionUtils.isNotEmpty(properties), "properties cannot be empty");
        this.sourceField = checkNotNull(sourceField, "source field cannot be null");
        this.targetField = checkNotNull(targetField, "target field cannot be null");
        this.properties = properties;
        this.tagsOnSuccess = tagsOnSuccess != null ? tagsOnSuccess : EMPTY_LIST;
    }

    @Override
    public ProcessResult process(Doc doc) {
        if (!doc.hasField(sourceField, String.class)) {
            return ProcessResult.failure(String.format("failed to get ip from [%s], field is missing or not instance of [%s]", sourceField, String.class));
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
            doc.appendList("tags", tagsOnSuccess);
        }

        return ProcessResult.success();
    }

    private Map<String, Object> extractGeoIp(InetAddress ipAddress) throws GeoIp2Exception, IOException {
        CityResponse response = databaseReader.city(ipAddress);

        if (LOCATION.getValue(response) == null) {
            return null;
        }

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

            return new GeoIpProcessor(TemplateService.compileTemplate(geoIpConfig.getSourceField()),
                    TemplateService.compileTemplate(geoIpConfig.getTargetField()),
                    geoIpConfig.getProperties(),
                    geoIpConfig.getTagsOnSuccess());
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String sourceField;
        private String targetField = "geoip";
        private List<Property> properties = ALL_PROPERTIES;
        private List<String> tagsOnSuccess = EMPTY_LIST;

        public Configuration() { }

        public Configuration(String sourceField, String targetField) {
            this.sourceField = sourceField;
            this.targetField = targetField;
        }

        public String getSourceField() {
            return sourceField;
        }

        public String getTargetField() {
            return targetField;
        }

        public List<Property> getProperties() {
            return properties;
        }

        public List<String> getTagsOnSuccess() {
            return tagsOnSuccess;
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
                Double longitude = response.getLocation().getLongitude();
                Double latitude = response.getLocation().getLatitude();
                if (longitude == null || latitude == null) {
                    return null;
                }
                return Arrays.asList(longitude, latitude);
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
