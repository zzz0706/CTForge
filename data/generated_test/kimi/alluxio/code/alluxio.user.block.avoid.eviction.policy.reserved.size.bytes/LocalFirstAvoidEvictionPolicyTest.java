package alluxio.client.block.policy;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;

import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;

public class LocalFirstAvoidEvictionPolicyTest {

  @Test
  public void DefaultReservedBytesIsZero() throws Exception {
    // 1. Create a fresh AlluxioConfiguration with no overrides
    AlluxioConfiguration conf = new InstancedConfiguration(
        alluxio.util.ConfigurationUtils.defaults()
    );

    // 2. Instantiate the policy under test
    LocalFirstAvoidEvictionPolicy policy = new LocalFirstAvoidEvictionPolicy(conf);

    // 3. Read mBlockCapacityReserved via reflection
    Field reservedField = LocalFirstAvoidEvictionPolicy.class
        .getDeclaredField("mBlockCapacityReserved");
    reservedField.setAccessible(true);
    long actualReserved = (long) reservedField.get(policy);

    // 4. Compute expected value from the configuration API
    long expectedReserved = conf.getBytes(
        PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES);

    // 5. Assert
    assertEquals(expectedReserved, actualReserved);
  }
}