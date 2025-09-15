package alluxio.client.block.policy;

import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Assert;
import org.junit.Test;

public class LocalFirstAvoidEvictionPolicyTest {

    @Test
    public void testEqualsWithDifferentPolicyInstance() {
        // Prepare the test conditions: Create two configurations with different reserved size values
        AlluxioProperties properties1 = new AlluxioProperties();
        InstancedConfiguration config1 = new InstancedConfiguration(properties1);
        config1.set(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES, "1048576"); // 1MB

        AlluxioProperties properties2 = new AlluxioProperties();
        InstancedConfiguration config2 = new InstancedConfiguration(properties2);
        config2.set(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES, "2097152"); // 2MB

        // Test code: Create two LocalFirstAvoidEvictionPolicy instances with the prepared configurations
        LocalFirstAvoidEvictionPolicy policy1 = new LocalFirstAvoidEvictionPolicy(config1);
        LocalFirstAvoidEvictionPolicy policy2 = new LocalFirstAvoidEvictionPolicy(config2);

        // Test the equals method
        boolean result = policy1.equals(policy2);

        // Assert the test result
        Assert.assertFalse("Policies with different reserved size values should not be equal", result);
    }
}