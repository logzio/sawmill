package io.logz.sawmill;

public class GeoIpConfiguration implements SawmillConfiguration {

    private final String geoIpDatabasePath;

    public GeoIpConfiguration(String geoIpDatabasePath) {
        this.geoIpDatabasePath = geoIpDatabasePath;
    }

    public String getGeoIpDatabasePath() {
        return geoIpDatabasePath;
    }
}
