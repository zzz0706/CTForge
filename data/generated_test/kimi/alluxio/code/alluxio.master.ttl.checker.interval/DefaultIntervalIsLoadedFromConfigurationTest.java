package alluxio.master.file.meta;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import static org.junit.Assert.assertEquals;

public class DefaultIntervalIsLoadedFromConfigurationTest {

    @Test
    public void defaultIntervalIsLoadedFromConfiguration() {
        // 1. Use the Alluxio 2.1.0 API to obtain configuration values.
        ServerConfiguration.reset(); // ensure clean JVM state

        // 2. Compute the expected value dynamically from the configuration.
        long expectedMs = ServerConfiguration.getMs(PropertyKey.MASTER_TTL_CHECKER_INTERVAL_MS);

        // 3. Trigger static initialization of TtlBucket.
        long actualMs = TtlBucket.getTtlIntervalMs();

        // 4. Assert the propagated value matches the configuration.
        assertEquals(expectedMs, actualMs);
    }
}