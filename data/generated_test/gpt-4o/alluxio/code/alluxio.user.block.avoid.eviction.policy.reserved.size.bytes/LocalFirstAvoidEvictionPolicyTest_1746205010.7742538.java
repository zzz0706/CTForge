package alluxio.client.block.policy;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.PropertyKey;
import org.junit.Assert;
import org.junit.Test;

public class LocalFirstAvoidEvictionPolicyTest {
  
    @Test
    // Test code
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_LocalFirstAvoidEvictionPolicy_EvictionBehavior() {
        // Step 1: Create AlluxioProperties and configure the reserved size property using the correct API
        AlluxioProperties properties = new AlluxioProperties();
        properties.set(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES, "1024");
        AlluxioConfiguration conf = new InstancedConfiguration(properties);

        // Step 2: Capture the configured reserved size value
        long reservedSizeBytes = conf.getBytes(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES);

        // Step 3: Initialize the LocalFirstAvoidEvictionPolicy instance using the configured value
        LocalFirstAvoidEvictionPolicy policy = new LocalFirstAvoidEvictionPolicy(conf);

        // Step 4: Test case for condition where eviction should proceed (capacity above reserved threshold)
        long totalUsedBytes = reservedSizeBytes + 1024L; // Used capacity above the threshold
        long bytesAboutToBeEvicted = 512L; // Simulate a small eviction
        boolean evictionOccurs = simulateEvictionDecision(policy, totalUsedBytes, bytesAboutToBeEvicted, reservedSizeBytes);
        Assert.assertTrue("Eviction should proceed when used capacity stays above the reserved threshold.", evictionOccurs);

        // Step 5: Test case for condition where eviction should be skipped (capacity below reserved threshold)
        totalUsedBytes = reservedSizeBytes - 512L; // Used capacity below the threshold
        bytesAboutToBeEvicted = 1024L; // Simulate a large eviction
        evictionOccurs = simulateEvictionDecision(policy, totalUsedBytes, bytesAboutToBeEvicted, reservedSizeBytes);
        Assert.assertFalse("Eviction should be skipped when reserved bytes threshold would be breached.", evictionOccurs);

        // Step 6: Verify the equals method behavior
        LocalFirstAvoidEvictionPolicy anotherPolicy = new LocalFirstAvoidEvictionPolicy(conf);
        Assert.assertTrue("Policies with identical configurations should be equal.", policy.equals(anotherPolicy));

        AlluxioProperties otherProperties = new AlluxioProperties();
        otherProperties.set(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES, "2048");
        AlluxioConfiguration otherConf = new InstancedConfiguration(otherProperties);
        LocalFirstAvoidEvictionPolicy differentPolicy = new LocalFirstAvoidEvictionPolicy(otherConf);
        Assert.assertFalse("Policies with different configurations should not be equal.", policy.equals(differentPolicy));
    }

    /**
     * Simulates the eviction decision logic for LocalFirstAvoidEvictionPolicy.
     * 
     * @param policy              The LocalFirstAvoidEvictionPolicy to test.
     * @param totalUsedBytes      The total bytes currently used.
     * @param bytesAboutToBeEvicted The bytes that are about to be evicted.
     * @param reservedSizeBytes   The reserved size threshold.
     * @return True if eviction should proceed, false otherwise.
     */
    private boolean simulateEvictionDecision(
            LocalFirstAvoidEvictionPolicy policy,
            long totalUsedBytes,
            long bytesAboutToBeEvicted,
            long reservedSizeBytes
    ) {
        // Mimic the policy's decision logic during eviction
        long afterEvictionBytes = totalUsedBytes - bytesAboutToBeEvicted;
        return afterEvictionBytes >= reservedSizeBytes;
    }
}