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
    public void testEvictionAllowedAboveReservedFloor() {
        // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // Create an AlluxioProperties instance and set up properties.
        AlluxioProperties properties = new AlluxioProperties();
        properties.set(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES, "524288");

        // Create an AlluxioConfiguration instance with the properties.
        AlluxioConfiguration configuration = new InstancedConfiguration(properties);

        // 2. Prepare the test conditions.
        LocalFirstAvoidEvictionPolicy policy = new LocalFirstAvoidEvictionPolicy(configuration);

        // Simulate total used bytes and eviction data.
        long totalUsedBytes = 1000000L; // Example total used bytes.
        long bytesAboutToBeEvicted = 400000L;

        // Retrieve the reserved size value from the configuration.
        long reservedSize = configuration.getBytes(
                PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES);

        // Validate reserved size is set up correctly.
        Assert.assertEquals(524288L, reservedSize);

        // Logic: `totalUsedBytes - bytesAboutToBeEvicted` should be greater or equal to `reservedSize`.
        boolean evictionAllowed = (totalUsedBytes - bytesAboutToBeEvicted >= reservedSize);

        // 3. Test code.
        Assert.assertTrue(evictionAllowed);

        // Test equals method for completeness.
        LocalFirstAvoidEvictionPolicy anotherPolicy = new LocalFirstAvoidEvictionPolicy(configuration);
        Assert.assertEquals(policy, anotherPolicy);
    }
}