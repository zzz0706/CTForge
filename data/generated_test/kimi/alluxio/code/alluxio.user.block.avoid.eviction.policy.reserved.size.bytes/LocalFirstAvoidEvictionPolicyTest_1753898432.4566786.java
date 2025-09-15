package alluxio.client.block.policy;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;

import com.google.common.base.Objects;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
    java.lang.reflect.Field reservedField = LocalFirstAvoidEvictionPolicy.class
        .getDeclaredField("mBlockCapacityReserved");
    reservedField.setAccessible(true);
    long actualReserved = (long) reservedField.get(policy);

    // 4. Compute expected value from the configuration API
    long expectedReserved = conf.getBytes(
        PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES);

    // 5. Assert
    assertEquals(expectedReserved, actualReserved);
  }

  @Test
  public void equalsWithDifferentReservedBytes() {
    // 1. Prepare two configurations with different reserved bytes
    InstancedConfiguration conf1 = new InstancedConfiguration(
        alluxio.util.ConfigurationUtils.defaults());
    InstancedConfiguration conf2 = new InstancedConfiguration(
        alluxio.util.ConfigurationUtils.defaults());
    conf2.set(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES, 1024L);

    // 2. Instantiate two policies
    LocalFirstAvoidEvictionPolicy policy1 = new LocalFirstAvoidEvictionPolicy(conf1);
    LocalFirstAvoidEvictionPolicy policy2 = new LocalFirstAvoidEvictionPolicy(conf2);

    // 3. Assert they are not equal
    assertFalse(policy1.equals(policy2));
  }

  @Test
  public void equalsWithSameReservedBytes() {
    // 1. Prepare two identical configurations
    AlluxioConfiguration conf = new InstancedConfiguration(
        alluxio.util.ConfigurationUtils.defaults());

    // 2. Instantiate two policies
    LocalFirstAvoidEvictionPolicy policy1 = new LocalFirstAvoidEvictionPolicy(conf);
    LocalFirstAvoidEvictionPolicy policy2 = new LocalFirstAvoidEvictionPolicy(conf);

    // 3. Assert they are equal
    assertTrue(policy1.equals(policy2));
  }
}