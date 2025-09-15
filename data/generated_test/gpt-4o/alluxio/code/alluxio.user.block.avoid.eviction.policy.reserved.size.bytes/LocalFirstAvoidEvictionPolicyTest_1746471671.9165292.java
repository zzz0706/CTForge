package alluxio.client.block.policy;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.AlluxioProperties;
import org.junit.Assert;
import org.junit.Test;
import alluxio.client.block.policy.LocalFirstAvoidEvictionPolicy;

public class LocalFirstAvoidEvictionPolicyTest {

    @Test
    public void testEqualsWithDifferentPolicyInstance() {
        // Test code
        // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        AlluxioProperties properties1 = new AlluxioProperties();
        InstancedConfiguration config1 = new InstancedConfiguration(properties1);
        config1.set(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES, "1048576");

        AlluxioProperties properties2 = new AlluxioProperties();
        InstancedConfiguration config2 = new InstancedConfiguration(properties2);
        config2.set(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES, "2097152");

        // 2. Prepare the test conditions
        LocalFirstAvoidEvictionPolicy policy1 = new LocalFirstAvoidEvictionPolicy(config1);
        LocalFirstAvoidEvictionPolicy policy2 = new LocalFirstAvoidEvictionPolicy(config2);

        // 3. Test code: Verify policy equality behavior based on configuration differences
        boolean result = policy1.equals(policy2);

        // 4. Code after testing: Assert the result to confirm expected behavior
        Assert.assertFalse("Policies with different reserved size values should not be equal", result);
    }
}