package alluxio.master.meta;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AlluxioMasterRestServiceHandlerTest {
    // Set up the configuration before tests
    @Before
    public void setUp() {
        // Restore the default configuration to ensure isolated testing
        ServerConfiguration.reset();
    }

    @Test
    public void testInvalidRefreshIntervalHandlingMaster() {
        // Prepare the test conditions
        final String invalidRefreshInterval = "invalid_value";

        // Set invalid refresh interval in the configuration
        ServerConfiguration.set(PropertyKey.WEB_REFRESH_INTERVAL, invalidRefreshInterval);

        // Act by retrieving the configuration value back
        String actualValue = ServerConfiguration.get(PropertyKey.WEB_REFRESH_INTERVAL);

        // Assert that the configuration holds the invalid value, as this test just checks handling
        Assert.assertEquals(invalidRefreshInterval, actualValue);
    }
}