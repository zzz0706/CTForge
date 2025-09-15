package alluxio.util;

import static org.junit.Assert.assertEquals;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

public class ConfigurationUtilsTest {

  @Test
  public void explicitJobMasterRpcAddresses() throws Exception {
    // 1. Obtain a fresh configuration instance via the Alluxio 2.1.0 API
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // 2. Prepare the test conditions: explicitly set JOB_MASTER_RPC_ADDRESSES
    conf.set(PropertyKey.JOB_MASTER_RPC_ADDRESSES, "host1:20001,host2:20002,host3:20003");

    // 3. Invoke the public method under test
    List<InetSocketAddress> actual = ConfigurationUtils.getJobMasterRpcAddresses(conf);

    // 4. Compute expected list using the same host/port strings
    List<InetSocketAddress> expected = Arrays.asList(
        new InetSocketAddress("host1", 20001),
        new InetSocketAddress("host2", 20002),
        new InetSocketAddress("host3", 20003)
    );

    // 5. Assert exact equality and ordering
    assertEquals(expected, actual);
  }
}