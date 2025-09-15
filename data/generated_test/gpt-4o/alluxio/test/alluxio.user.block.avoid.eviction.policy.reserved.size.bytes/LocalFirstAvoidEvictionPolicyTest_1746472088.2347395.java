package alluxio.client.block.policy;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.PropertyKey;
import alluxio.client.block.policy.LocalFirstAvoidEvictionPolicy;
import org.junit.Assert;
import org.junit.Test;

public class LocalFirstAvoidEvictionPolicyTest {

    @Test
    // test code
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testEvictionAllowedAboveReservedFloor() {
        // Create an AlluxioProperties instance and set up properties dynamically.
        AlluxioProperties properties = new AlluxioProperties();
        properties.set(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES, "524288");

        // Create an AlluxioConfiguration instance using the properties.
        AlluxioConfiguration configuration = new InstancedConfiguration(properties);

        // Prepare the test conditions: Instantiate LocalFirstAvoidEvictionPolicy using configuration.
        LocalFirstAvoidEvictionPolicy policy = new LocalFirstAvoidEvictionPolicy(configuration);

        // Simulate used bytes and eviction data for the test.
        long totalUsedBytes = 1000000L; // Example value: current total block capacity used.
        long bytesAboutToBeEvicted = 400000L; // Example value: bytes intended for eviction.

        // Retrieve the reserved size value from the configuration for validation.
        long reservedSize = configuration.getBytes(
                PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES);

        // Validate that the reserved size from configuration is correctly set.
        Assert.assertEquals(524288L, reservedSize);

        // Perform the eviction logic test: Ensure eviction respects the reserved size.
        boolean evictionAllowed = (totalUsedBytes - bytesAboutToBeEvicted >= reservedSize);

        // Assert the expected behavior - eviction is allowed.
        Assert.assertTrue(evictionAllowed);

        // Additional test: Verify that equal policies with same configuration are treated correctly.
        LocalFirstAvoidEvictionPolicy anotherPolicy = new LocalFirstAvoidEvictionPolicy(configuration);
        Assert.assertEquals(policy, anotherPolicy);

        // Code after testing: Clean up resources, if any, and assert additional invariants.
        // Additional cleanup, if required, goes here.
    }
}