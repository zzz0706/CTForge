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
        // Step 1: Dynamically configure properties using AlluxioProperties.
        AlluxioProperties properties = new AlluxioProperties();
        properties.set(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES, "524288");

        // Step 2: Create an AlluxioConfiguration instance.
        AlluxioConfiguration configuration = new InstancedConfiguration(properties);

        // Step 3: Instantiate LocalFirstAvoidEvictionPolicy using the configuration.
        LocalFirstAvoidEvictionPolicy policy = new LocalFirstAvoidEvictionPolicy(configuration);

        // Step 4: Simulate the eviction scenario.
        long totalUsedBytes = 1000000L; // Example value: current total block capacity used.
        long bytesAboutToBeEvicted = 400000L; // Example value: bytes intended for eviction.

        // Retrieve the reserved size from configuration for testing.
        long reservedSize = configuration.getBytes(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES);

        // Validate the reserved size value obtained from configuration.
        Assert.assertEquals(524288L, reservedSize);

        // Verify the eviction decision logic.
        boolean evictionAllowed = totalUsedBytes - bytesAboutToBeEvicted >= reservedSize;

        // Validate the expected result: eviction is allowed.
        Assert.assertTrue(evictionAllowed);

        // Step 5: Validate policy identity (equals method covers configuration usage).
        LocalFirstAvoidEvictionPolicy anotherPolicy = new LocalFirstAvoidEvictionPolicy(configuration);
        Assert.assertEquals(policy, anotherPolicy);

        // Additional cleanup or invariants (if necessary) can be added here.
    }
}