package alluxio.conf;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.PropertyKey;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConfigurationValidationTest {

    /**
     * Test to validate the configuration value for alluxio.user.file.create.ttl.action.
     */
    @Test
    public void testUserFileCreateTtlActionConfiguration() {
        // Step 1: Prepare test conditions by initializing the necessary AlluxioProperties
        AlluxioProperties properties = new AlluxioProperties();

        // Step 2: Add valid configuration data into properties
        properties.set(PropertyKey.USER_FILE_CREATE_TTL_ACTION, "DELETE");

        // Step 3: Obtain the Alluxio configuration using the correct constructor
        AlluxioConfiguration conf = new InstancedConfiguration(properties);

        try {
            // Step 4: Retrieve the configuration value for alluxio.user.file.create.ttl.action
            String ttlAction = conf.get(PropertyKey.USER_FILE_CREATE_TTL_ACTION);

            // Step 5: Validate the value against the defined constraints (DELETE or FREE)
            assertTrue("Invalid value for alluxio.user.file.create.ttl.action: " + ttlAction,
                    "DELETE".equals(ttlAction) || "FREE".equals(ttlAction));

        } catch (Exception e) {
            // Handle unexpected exceptions
            fail("An error occurred while validating alluxio.user.file.create.ttl.action configuration: " +
                    e.getMessage());
        }
    }
}