package io.logz.sawmill.processors;

import com.maxmind.db.InvalidDatabaseException;
import io.logz.sawmill.exceptions.SawmillException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GeoIpDbReaderFactoryTest {

    @Test
    public void shouldCreateDbReaderFromMmdb() throws Exception {
        assertNotNull(GeoIpDbReaderFactory.createDatabaseReader("GeoIP2-City-Test.mmdb"));
    }

    @Test
    public void shouldCreateDbReaderFromTarGz() throws Exception {
        assertNotNull(GeoIpDbReaderFactory.createDatabaseReader("GeoIP2-City-Test.tar.gz"));
    }

    @Test
    public void shouldThrowOnTarGzDoesNotContainMmdb() throws Exception {
        String location = "GeoIP2-City-Test-broken.tar.gz";
        try {
            GeoIpDbReaderFactory.createDatabaseReader(location);
            fail();
        } catch (SawmillException e) {
            assertEquals("Failed to load geoip database from '" + location + "'", e.getMessage());
            Throwable cause = e.getCause();
            assertEquals("Could not find a valid .mmdb file within archive", cause.getMessage());
        }
    }

    @Test
    public void shouldThrowOnMmdbFileDoesNotExist() throws Exception {
        String location = "non-existing-file";
        try {
            GeoIpDbReaderFactory.createDatabaseReader(location);
            fail();
        } catch (SawmillException e) {
            assertEquals("Failed to load geoip database from '" + location + "'", e.getMessage());
            assertTrue(e.getCause() instanceof IllegalArgumentException);
        }
    }

    @Test
    public void shouldThrowOnInvalidMmdbFile() throws Exception {
        String location = "LICENSE";
        try {
            GeoIpDbReaderFactory.createDatabaseReader(location);
            fail();
        } catch (SawmillException e) {
            assertEquals("Failed to load geoip database from '" + location + "'", e.getMessage());
            assertTrue(e.getCause() instanceof InvalidDatabaseException);
        }
    }
}
