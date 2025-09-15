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
    // 1. Obtain a fresh configuration instance using the Alluxio 2.1.0 API
    AlluxioConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // 2. Prepare the test condition: inject a malformed address list
    ((InstancedConfiguration) conf).set(PropertyKey.JOB_MASTER_RPC_ADDRESSES, "host1:abc,host2:20002");

    // 3. Test code: expect an IllegalArgumentException when parsing the malformed port "abc"
    mThrown.expect(IllegalArgumentException.class);
    ConfigurationUtils.getJobMasterRpcAddresses(conf);

    // 4. Nothing to clean up; the exception is the expected result
  }
}