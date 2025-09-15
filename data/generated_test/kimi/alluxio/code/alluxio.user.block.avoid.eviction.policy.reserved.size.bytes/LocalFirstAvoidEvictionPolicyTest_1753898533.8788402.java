package alluxio.client.block.policy;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

public class LocalFirstAvoidEvictionPolicyTest {

  @Test
  public void customReservedBytesIsUsed() throws Exception {
    // 1. Use Alluxio 2.1.0 API to load the configuration file
    AlluxioConfiguration conf = new InstancedConfiguration(
        ConfigurationUtils.defaults());

    // 2. Prepare test conditions: override the property with 512 MB
    ((InstancedConfiguration) conf).set(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES, "512MB");

    // 3. Instantiate the policy with the configuration
    LocalFirstAvoidEvictionPolicy policy =
        new LocalFirstAvoidEvictionPolicy(conf);

    // 4. Read the internal field via reflection
    Field reservedField =
        LocalFirstAvoidEvictionPolicy.class.getDeclaredField("mBlockCapacityReserved");
    reservedField.setAccessible(true);
    long actualReserved = (Long) reservedField.get(policy);

    // 5. Calculate expected value dynamically
    long expectedReserved = 512L * 1024 * 1024; // 512 MB in bytes

    // 6. Assert the result
    Assert.assertEquals(expectedReserved, actualReserved);
  }
}