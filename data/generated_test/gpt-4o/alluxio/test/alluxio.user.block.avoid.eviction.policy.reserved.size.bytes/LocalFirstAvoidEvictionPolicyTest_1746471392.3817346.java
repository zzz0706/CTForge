package alluxio.client.block.policy;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.PropertyKey;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class LocalFirstAvoidEvictionPolicyTest {

    @Test
    //test code
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    public void testEqualsWithSamePolicyInstance() {
        // Step 1: Prepare the configuration with identical property values using the Alluxio API.
        AlluxioProperties props1 = new AlluxioProperties();
        AlluxioProperties props2 = new AlluxioProperties();
        
        // Set 'alluxio.user.block.avoid.eviction.policy.reserved.size.bytes' to the same value in both configurations.
        String reservedSizeBytesValue = "2097152"; // Setting the value to 2MB dynamically
        props1.set(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES, reservedSizeBytesValue);
        props2.set(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES, reservedSizeBytesValue);

        AlluxioConfiguration conf1 = new InstancedConfiguration(props1);
        AlluxioConfiguration conf2 = new InstancedConfiguration(props2);

        // Step 2: Instantiate two LocalFirstAvoidEvictionPolicy instances using their respective configurations.
        LocalFirstAvoidEvictionPolicy policy1 = new LocalFirstAvoidEvictionPolicy(conf1);
        LocalFirstAvoidEvictionPolicy policy2 = new LocalFirstAvoidEvictionPolicy(conf2);

        // 3. Test code.
        // Invoke the equals method to compare the two instances.
        boolean areEqual = policy1.equals(policy2);

        // 4. Code after testing.
        // Validate that the two policy instances are considered equal.
        assertTrue("Expected the two instances to be equal.", areEqual);
    }
}