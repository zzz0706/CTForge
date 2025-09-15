package alluxio.util;

import static org.junit.Assert.assertEquals;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;

import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

public class ConfigurationUtilsTest {

  @Test
  public void testFallbackToMasterRpcAddressesWithPortOverride() {
    // 1. Create a new AlluxioConfiguration instance.
    AlluxioConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // 2. Unset PropertyKey.JOB_MASTER_RPC_ADDRESSES.
    ((InstancedConfiguration) conf).unset(PropertyKey.JOB_MASTER_RPC_ADDRESSES);

    // 3. Set PropertyKey.MASTER_RPC_ADDRESSES to `master1:19998,master2:19998`.
    ((InstancedConfiguration) conf).set(PropertyKey.MASTER_RPC_ADDRESSES, "master1:19998,master2:19998");

    // 4. Set PropertyKey.JOB_MASTER_RPC_PORT to 30001.
    ((InstancedConfiguration) conf).set(PropertyKey.JOB_MASTER_RPC_PORT, 30001);

    // 5. Call ConfigurationUtils.getJobMasterRpcAddresses(conf).
    List<InetSocketAddress> actual = ConfigurationUtils.getJobMasterRpcAddresses(conf);

    // 6. Assert the returned list equals [master1/30001, master2/30001].
    List<InetSocketAddress> expected = Arrays.asList(
        InetSocketAddress.createUnresolved("master1", 30001),
        InetSocketAddress.createUnresolved("master2", 30001)
    );
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i).getHostString(), actual.get(i).getHostString());
      assertEquals(expected.get(i).getPort(), actual.get(i).getPort());
    }
  }
}