package io.logz.sawmill.processors;

import io.logz.sawmill.exceptions.SawmillException;
import org.junit.Test;

public class GeoIpDbReaderFactoryTest {

    @Test
    public void shouldCreateDbReaderFromMmdb() throws Exception {
        GeoIpDbReaderFactory.createDatabaseReader("GeoIP2-City-Test.mmdb");
    }

    @Test
    public void shouldCreateDbReaderFromTarGz() throws Exception {
        GeoIpDbReaderFactory.createDatabaseReader("GeoIP2-City-Test.tar.gz");
    }

    @Test(expected = SawmillException.class)
    public void shouldThrowOnTarGzDoesNotContainMmdb() throws Exception {
        GeoIpDbReaderFactory.createDatabaseReader("GeoIP2-City-Test-broken.tar.gz");
    }

    @Test(expected = SawmillException.class)
    public void shouldThrowOnMmdbFileDoesNotExist() throws Exception {
        GeoIpDbReaderFactory.createDatabaseReader("non-existing-file");
    }

    @Test(expected = SawmillException.class)
    public void shouldThrowOnInvalidMmdbFile() throws Exception {
        GeoIpDbReaderFactory.createDatabaseReader("LICENSE");
    }
}
