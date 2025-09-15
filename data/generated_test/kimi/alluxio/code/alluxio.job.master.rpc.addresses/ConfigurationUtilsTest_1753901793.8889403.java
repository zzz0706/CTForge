package alluxio.util;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;

import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

public class ConfigurationUtilsTest {

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Test
  public void malformedExplicitAddressThrows() {
    // 1. Create a new AlluxioConfiguration instance
    AlluxioConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // 2. Set the malformed value directly (simulates a user-provided bad entry)
    ((InstancedConfiguration) conf).set(PropertyKey.JOB_MASTER_RPC_ADDRESSES, "host1:abc,host2:20002");

    // 3. Expect an IllegalArgumentException due to the invalid port 'abc'
    mThrown.expect(IllegalArgumentException.class);

    // 4. Invoke the method under test
    ConfigurationUtils.getJobMasterRpcAddresses(conf);
  }
}