package alluxio.util;

import static org.junit.Assert.assertEquals;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;

import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

public class ConfigurationUtilsTest {

  @Test
  public void explicitJobMasterRpcAddresses() {
    // 1. Instantiate a fresh configuration instance
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // 2. Prepare the test conditions: explicitly set the property
    conf.set(PropertyKey.JOB_MASTER_RPC_ADDRESSES, "host1:20001,host2:20002,host3:20003");

    // 3. Invoke the method under test
    List<InetSocketAddress> actual =
        ConfigurationUtils.getJobMasterRpcAddresses(conf);

    // 4. Compute expected values dynamically
    List<InetSocketAddress> expected = Arrays.asList(
        new InetSocketAddress("host1", 20001),
        new InetSocketAddress("host2", 20002),
        new InetSocketAddress("host3", 20003)
    );

    // 5. Assertions
    assertEquals(expected, actual);
  }
}