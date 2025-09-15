package alluxio.client.block.policy;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class LocalFirstAvoidEvictionPolicyConfigValidationTest {

  private AlluxioConfiguration mConf;

  @Before
  public void before() {
    // 1. Use the alluxio2.1.0 API to obtain configuration values instead of hard-coding them.
    mConf = new InstancedConfiguration(ConfigurationUtils.defaults());
  }

  @After
  public void after() {
    // 4. Clean up after test.
  }

  @Test
  public void validateAvoidEvictionPolicyReservedSize() {
    // 2. Prepare test conditions: retrieve the configuration value.
    long reservedBytes = mConf.getBytes(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES);

    // 3. Test code: validate the value against its constraints.
    // The configuration represents a *portion* of space reserved; therefore it must be non-negative.
    assertTrue("alluxio.user.block.avoid.eviction.policy.reserved.size.bytes must be non-negative",
        reservedBytes >= 0);
  }
}