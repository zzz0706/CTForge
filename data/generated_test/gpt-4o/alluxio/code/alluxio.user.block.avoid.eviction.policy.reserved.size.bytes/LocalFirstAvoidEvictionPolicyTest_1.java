package alluxio.client.block.policy;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.PropertyKey;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class LocalFirstAvoidEvictionPolicyTest {
    @Test
    public void testEqualsWithSamePolicyInstance() {
        // Prepare the test conditions

        // Create AlluxioProperties and set configuration values
        AlluxioProperties props1 = new AlluxioProperties();
        props1.set(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES, "2097152"); // 2MB
        AlluxioConfiguration conf1 = new InstancedConfiguration(props1);

        AlluxioProperties props2 = new AlluxioProperties();
        props2.set(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES, "2097152"); // 2MB
        AlluxioConfiguration conf2 = new InstancedConfiguration(props2);

        // Instantiate two LocalFirstAvoidEvictionPolicy instances using the respective configurations
        LocalFirstAvoidEvictionPolicy policy1 = new LocalFirstAvoidEvictionPolicy(conf1);
        LocalFirstAvoidEvictionPolicy policy2 = new LocalFirstAvoidEvictionPolicy(conf2);

        // Test code

        // Invoke the equals method to compare the two instances
        boolean areEqual = policy1.equals(policy2);

        // Code after testing

        // Verify that the two policy instances are considered equal
        assertTrue("Expected the two instances to be equal.", areEqual);
    }
}