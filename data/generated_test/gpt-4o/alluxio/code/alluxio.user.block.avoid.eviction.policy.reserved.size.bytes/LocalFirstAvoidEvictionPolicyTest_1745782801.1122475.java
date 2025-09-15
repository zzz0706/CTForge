package alluxio.client.block.policy;

import alluxio.client.block.policy.LocalFirstAvoidEvictionPolicy;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LocalFirstAvoidEvictionPolicyTest {

    private AlluxioConfiguration conf1;
    private AlluxioConfiguration conf2;

    @Before
    public void setup() {
        // Initialize AlluxioProperties for each configuration
        AlluxioProperties properties1 = new AlluxioProperties();
        AlluxioProperties properties2 = new AlluxioProperties();

        // Instantiate InstancedConfiguration for testing
        conf1 = new InstancedConfiguration(properties1);
        conf2 = new InstancedConfiguration(properties2);

        // Set properties dynamically to simulate distinct configurations
        properties1.set(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES, "1024");
        properties2.set(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES, "2048");
    }

    @Test
    public void test_LocalFirstAvoidEvictionPolicy_equality_validation() {
        // 1. Fetch configuration values dynamically using the API
        long reservedBytes1 = conf1.getBytes(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES);
        long reservedBytes2 = conf2.getBytes(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES);

        // 2. Prepare conditions: Create instances of LocalFirstAvoidEvictionPolicy
        LocalFirstAvoidEvictionPolicy policy1 = new LocalFirstAvoidEvictionPolicy(reservedBytes1, conf1);
        LocalFirstAvoidEvictionPolicy policy2 = new LocalFirstAvoidEvictionPolicy(reservedBytes2, conf2);

        // 3. Test: Validate that the policies with differing configurations are unequal
        Assert.assertFalse("Policies should not be equal with differing reserved sizes",
            policy1.equals(policy2));

        // 4. Create another policy instance with identical configuration to policy1
        LocalFirstAvoidEvictionPolicy policy3 = new LocalFirstAvoidEvictionPolicy(reservedBytes1, conf1);

        // Validate that policies with the identical configuration are equal
        Assert.assertTrue("Policies should be equal with identical reserved sizes and configurations",
            policy1.equals(policy3));
    }
}