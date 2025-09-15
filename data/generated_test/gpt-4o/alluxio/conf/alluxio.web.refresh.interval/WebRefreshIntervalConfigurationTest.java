package alluxio.conf;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Test;
import static org.junit.Assert.*;

public class WebRefreshIntervalConfigurationTest {
    @Test
    public void testWebRefreshIntervalConfigurationValidity() {
        // Prepare the test conditions: Initialize configuration
        AlluxioConfiguration configuration = InstancedConfiguration.defaults();

        // Test code: Verify the configuration value
        try {
            // Fetch the configuration value using Alluxio API
            long refreshIntervalInMs = configuration.getMs(PropertyKey.WEB_REFRESH_INTERVAL);

            // Constraint 1: Ensure the value is positive
            assertTrue("WEB_REFRESH_INTERVAL must be positive.", refreshIntervalInMs > 0);

            // Constraint 2: Ensure the value is within a reasonable range
            assertTrue("WEB_REFRESH_INTERVAL should not exceed an hour.", refreshIntervalInMs <= 3600000);
            assertTrue("WEB_REFRESH_INTERVAL should be at least 1 second.", refreshIntervalInMs >= 1000);

        } catch (IllegalArgumentException e) {
            fail("Invalid configuration encountered for WEB_REFRESH_INTERVAL: " + e.getMessage());
        }
    }
}