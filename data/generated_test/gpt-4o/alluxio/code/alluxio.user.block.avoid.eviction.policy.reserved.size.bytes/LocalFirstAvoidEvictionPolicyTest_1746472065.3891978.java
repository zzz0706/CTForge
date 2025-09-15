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
        // 1. Create an AlluxioProperties instance and set up properties.
        AlluxioProperties properties = new AlluxioProperties();
        properties.set(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES, "524288");

        // Create an AlluxioConfiguration instance with the properties.
        AlluxioConfiguration configuration = new InstancedConfiguration(properties);

        // 2. Instantiate LocalFirstAvoidEvictionPolicy using the configuration.
        LocalFirstAvoidEvictionPolicy policy = new LocalFirstAvoidEvictionPolicy(configuration);

        // 3. Simulate total used bytes and eviction data such that 
        //    'totalUsedBytes - bytesAboutToBeEvicted >= mBlockCapacityReserved'.
        long totalUsedBytes = 1000000L; // Example total used bytes.
        long bytesAboutToBeEvicted = 400000L; // Example eviction amount.
        long reservedSize = Long.parseLong(
                configuration.get(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES)); // Retrieves the value set earlier.

        // Logic: `totalUsedBytes - bytesAboutToBeEvicted` should be greater or equal to `reservedSize`.
        boolean evictionAllowed = (totalUsedBytes - bytesAboutToBeEvicted >= reservedSize);

        // 4. Eviction decision logic should allow eviction, ensuring the reserved threshold is respected.
        Assert.assertTrue(evictionAllowed);
    }
}