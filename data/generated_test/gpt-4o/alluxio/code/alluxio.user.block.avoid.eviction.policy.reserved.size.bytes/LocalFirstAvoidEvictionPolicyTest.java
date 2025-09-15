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

        // Set specific configurations for testing if needed
        properties1.set(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES, "1024");
        properties2.set(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES, "2048");
    }

    @Test
    public void test_LocalFirstAvoidEvictionPolicy_equality_validation() {
        // Obtain configuration values using the Alluxio API
        long reservedBytes1 = conf1.getBytes(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES);
        long reservedBytes2 = conf2.getBytes(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES);

        // Create policy instances with respective configurations
        LocalFirstAvoidEvictionPolicy policy1 = new LocalFirstAvoidEvictionPolicy(reservedBytes1, conf1);
        LocalFirstAvoidEvictionPolicy policy2 = new LocalFirstAvoidEvictionPolicy(reservedBytes2, conf2);

        // Test equality for the cases with potentially different reserved bytes
        boolean arePoliciesEqual = policy1.equals(policy2);

        // Assert that the policies are not equal when configurations differ
        Assert.assertFalse(arePoliciesEqual);

        // Simulate matching configurations
        LocalFirstAvoidEvictionPolicy policy3 = new LocalFirstAvoidEvictionPolicy(reservedBytes1, conf1);

        // Test equality for cases with matching configurations
        boolean arePoliciesIdentical = policy1.equals(policy3);

        // Assert that policies are equal when both configurations are identical
        Assert.assertTrue(arePoliciesIdentical);
    }
}