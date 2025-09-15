package alluxio.client.block.policy;

import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.client.block.policy.LocalFirstAvoidEvictionPolicy;
import org.junit.Assert;
import org.junit.Test;

public class LocalFirstAvoidEvictionPolicyTest {

    @Test
    public void testEqualsWithDifferentPolicyInstance() {
        // Test code
        // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // Prepare the test conditions: Create two configurations with different reserved size values using AlluxioProperties.

        AlluxioProperties properties1 = new AlluxioProperties();
        InstancedConfiguration config1 = new InstancedConfiguration(properties1);
        config1.set(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES, "1048576"); // Configure reserved size using Alluxio API

        AlluxioProperties properties2 = new AlluxioProperties();
        InstancedConfiguration config2 = new InstancedConfiguration(properties2);
        config2.set(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES, "2097152"); // Configure reserved size using Alluxio API

        // 2. Prepare the test conditions by instantiating the policies with configurations
        LocalFirstAvoidEvictionPolicy policy1 = new LocalFirstAvoidEvictionPolicy(config1);
        LocalFirstAvoidEvictionPolicy policy2 = new LocalFirstAvoidEvictionPolicy(config2);

        // 3. Test the equals method
        boolean result = policy1.equals(policy2);

        // 4. Code after testing: Assert the result to ensure correctness
        Assert.assertFalse("Policies with different reserved size values should not be equal", result);
    }
}