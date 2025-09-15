package alluxio.client.block.policy;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Assert;
import org.junit.Test;

public class LocalFirstAvoidEvictionPolicyTest {

    @Test
    public void testReservedSizeBytesConfig() {
        // Create an instance of AlluxioConfiguration to read configurations
        AlluxioConfiguration conf = InstancedConfiguration.defaults();

        // Step 1: Get the configuration value using the API
        String reservedSizeStr = conf.get(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES);

        // Step 2: Validate the configuration value
        try {
            // Check if the configuration value is non-null and a valid byte size string
            if (reservedSizeStr == null) {
                throw new IllegalArgumentException("Configuration value is null");
            }

            // Convert the configuration value to bytes
            long reservedSizeBytes = conf.getBytes(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES);

            // Step 3: Verify constraints: the value must not be negative
            Assert.assertTrue(
                "The configuration 'alluxio.user.block.avoid.eviction.policy.reserved.size.bytes' must be non-negative.",
                reservedSizeBytes >= 0
            );
        } catch (IllegalArgumentException e) {
            Assert.fail("Failed to parse 'alluxio.user.block.avoid.eviction.policy.reserved.size.bytes': " + e.getMessage());
        }
    }
}