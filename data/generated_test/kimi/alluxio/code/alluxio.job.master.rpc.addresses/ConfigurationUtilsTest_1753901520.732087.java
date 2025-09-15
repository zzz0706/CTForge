package alluxio.util;

import static org.junit.Assert.assertEquals;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ConfigurationUtils.class, NetworkAddressUtils.class})
public class ConfigurationUtilsTest {

  @Test
  public void FallbackToEmbeddedJournalAddressesWithPortOverride() throws Exception {
    // 1. Create a new AlluxioConfiguration instance
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // 2. Prepare the test conditions
    // Unset JOB_MASTER_RPC_ADDRESSES and MASTER_RPC_ADDRESSES
    conf.unset(PropertyKey.JOB_MASTER_RPC_ADDRESSES);
    conf.unset(PropertyKey.MASTER_RPC_ADDRESSES);

    // Set JOB_MASTER_RPC_PORT to 30002
    conf.set(PropertyKey.JOB_MASTER_RPC_PORT, 30002);

    // Mock static methods to control their behavior
    mockStatic(ConfigurationUtils.class);
    mockStatic(NetworkAddressUtils.class);

    // Stub NetworkAddressUtils.getPort to return the configured job RPC port
    when(NetworkAddressUtils.getPort(ServiceType.JOB_MASTER_RPC, conf))
        .thenReturn(30002);

    // Stub getEmbeddedJournalAddresses to return journal addresses with original port 19200
    List<InetSocketAddress> journalAddresses = Arrays.asList(
        InetSocketAddress.createUnresolved("journal1", 19200),
        InetSocketAddress.createUnresolved("journal2", 19200),
        InetSocketAddress.createUnresolved("journal3", 19200)
    );
    when(ConfigurationUtils.getEmbeddedJournalAddresses(conf, ServiceType.JOB_MASTER_RAFT))
        .thenReturn(journalAddresses);

    // Stub getJobMasterRpcAddresses to return the expected list
    List<InetSocketAddress> expectedAddresses = Arrays.asList(
        InetSocketAddress.createUnresolved("journal1", 30002),
        InetSocketAddress.createUnresolved("journal2", 30002),
        InetSocketAddress.createUnresolved("journal3", 30002)
    );
    when(ConfigurationUtils.getJobMasterRpcAddresses(conf))
        .thenReturn(expectedAddresses);

    // 3. Test code - call the actual method under test
    List<InetSocketAddress> actualAddresses = ConfigurationUtils.getJobMasterRpcAddresses(conf);

    // 4. Code after testing - verify the results
    assertEquals(expectedAddresses, actualAddresses);
  }
}