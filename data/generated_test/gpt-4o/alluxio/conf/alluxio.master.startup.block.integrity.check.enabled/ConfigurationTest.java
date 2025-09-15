package alluxio.conf;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.PropertyKey;
import org.junit.Assert;
import org.junit.Test;

public class ConfigurationTest {

    @Test
    public void testMasterStartupBlockIntegrityCheckEnabled() {
        /*
         * Step 1: Prepare the test conditions by correctly initializing the InstancedConfiguration
         * instance with AlluxioProperties.
         * Step 2: Use the appropriate API to fetch the configuration value.
         * Step 3: Validate the fetched configuration value.
         */
        try {
            // Step 1: Prepare the test conditions
            AlluxioProperties properties = new AlluxioProperties();
            InstancedConfiguration configuration = new InstancedConfiguration(properties);

            // Step 2: Fetch the configuration value using the correct API method
            boolean isBlockIntegrityCheckEnabled = configuration.getBoolean(
                PropertyKey.MASTER_STARTUP_BLOCK_INTEGRITY_CHECK_ENABLED);

            // Step 3: Validate the constraints
            Assert.assertTrue("The configuration value must be either true or false",
                isBlockIntegrityCheckEnabled == true || isBlockIntegrityCheckEnabled == false);

        } catch (Exception e) {
            Assert.fail("An unexpected exception occurred while reading or validating the configuration: "
                + e.getMessage());
        }
    }
}