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
        // Initialize AlluxioProperties to avoid passing null to the constructor
        AlluxioProperties properties1 = new AlluxioProperties();
        AlluxioProperties properties2 = new AlluxioProperties();

        // Create instances of InstancedConfiguration using the required constructors
        conf1 = new InstancedConfiguration(properties1);
        conf2 = new InstancedConfiguration(properties2);

        // Set specific configurations dynamically for testing
        properties1.set(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES, "1024");
        properties2.set(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES, "2048");
    }

    @Test
    public void test_LocalFirstAvoidEvictionPolicy_equality_validation() {
        // 1. Use the API for obtaining configuration values dynamically
        long reservedBytes1 = conf1.getBytes(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES);
        long reservedBytes2 = conf2.getBytes(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES);

        // 2. Prepare the test conditions: Create policy instances with configurations
        LocalFirstAvoidEvictionPolicy policy1 = new LocalFirstAvoidEvictionPolicy(reservedBytes1, conf1);
        LocalFirstAvoidEvictionPolicy policy2 = new LocalFirstAvoidEvictionPolicy(reservedBytes2, conf2);

        // 3. Test: Ensure equality is false for different configurations
        Assert.assertFalse("Policies should not be equal when configurations differ", policy1.equals(policy2));

        // Duplicate from the original configuration
        LocalFirstAvoidEvictionPolicy policy3 = new LocalFirstAvoidEvictionPolicy(reservedBytes1, conf1);

        // 4. Test: Ensure equality is true for matching configurations
        Assert.assertTrue("Policies should be equal when configurations match", policy1.equals(policy3));
    }
}