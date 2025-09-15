package alluxio.client.block.policy;

import alluxio.client.block.policy.LocalFirstAvoidEvictionPolicy;
import alluxio.conf.PropertyKey;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LocalFirstAvoidEvictionPolicyTest {

    private AlluxioConfiguration conf1;
    private AlluxioConfiguration conf2;
    private AlluxioConfiguration conf3;

    @Before
    public void setup() {
        // Initialize AlluxioProperties for each configuration
        AlluxioProperties properties1 = new AlluxioProperties();
        AlluxioProperties properties2 = new AlluxioProperties();
        AlluxioProperties properties3 = new AlluxioProperties();

        // Instantiate InstancedConfiguration for testing
        conf1 = new InstancedConfiguration(properties1);
        conf2 = new InstancedConfiguration(properties2);
        conf3 = new InstancedConfiguration(properties3);

        // Dynamically set properties to simulate distinct configurations
        properties1.set(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES, "1024");
        properties2.set(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES, "2048");
        properties3.set(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES, "1024");
    }

    @Test
    public void test_LocalFirstAvoidEvictionPolicy_equality_validation() {
        // 1. Use the API to retrieve configuration values dynamically
        long reservedBytes1 = conf1.getBytes(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES);
        long reservedBytes2 = conf2.getBytes(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES);
        long reservedBytes3 = conf3.getBytes(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES);

        // 2. Prepare the test conditions: Create instances of LocalFirstAvoidEvictionPolicy
        LocalFirstAvoidEvictionPolicy policy1 = new LocalFirstAvoidEvictionPolicy(conf1);
        LocalFirstAvoidEvictionPolicy policy2 = new LocalFirstAvoidEvictionPolicy(conf2);
        LocalFirstAvoidEvictionPolicy policy3 = new LocalFirstAvoidEvictionPolicy(conf3);

        // 3. Validate that policies with different configurations are unequal
        Assert.assertFalse("Policies should not be equal if reserved sizes are different",
            policy1.equals(policy2));

        // 4. Validate that policies with identical configurations are equal
        Assert.assertTrue("Policies should be equal if reserved sizes are identical",
            policy1.equals(policy3));
    }
}