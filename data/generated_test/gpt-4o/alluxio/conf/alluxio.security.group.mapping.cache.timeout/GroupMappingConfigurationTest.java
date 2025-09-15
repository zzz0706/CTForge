package alluxio.security.group;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.PropertyKey;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit test for verifying the configuration value of alluxio.security.group.mapping.cache.timeout.
 */
public class GroupMappingConfigurationTest {

    @Test
    public void testSecurityGroupMappingCacheTimeoutConfiguration() {
        // 1. Initialize Alluxio properties with the expected configuration key and value.
        AlluxioProperties properties = new AlluxioProperties();
        properties.set(PropertyKey.SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS, "1000"); // Example configuration value

        // 2. Create an InstancedConfiguration with the initialized properties.
        InstancedConfiguration conf = new InstancedConfiguration(properties);

        // 3. Obtain the configuration value using the correct Alluxio API.
        String timeoutConfig = conf.get(PropertyKey.SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS);

        // 4. Ensure the configuration value is not null or empty.
        assertNotNull("Configuration value for SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS should not be null", timeoutConfig);
        assertFalse("Configuration value for SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS should not be empty", timeoutConfig.isEmpty());

        // 5. Parse the configuration value and validate it.
        try {
            long timeoutMs = Long.parseLong(timeoutConfig);
            assertTrue("Configuration value for SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS should be positive", timeoutMs > 0);
        } catch (NumberFormatException e) {
            fail("Configuration value for SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS should be a valid number");
        }
    }
}