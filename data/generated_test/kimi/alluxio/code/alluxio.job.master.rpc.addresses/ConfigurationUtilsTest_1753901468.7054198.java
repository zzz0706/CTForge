package alluxio.util;

import static org.junit.Assert.assertEquals;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import alluxio.util.network.NetworkAddressUtils;

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
    // 1. Create configuration without setting JOB_MASTER_RPC_ADDRESSES or MASTER_RPC_ADDRESSES
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // 2. Prepare the test conditions
    int expectedPort = 30002;
    conf.set(PropertyKey.JOB_MASTER_RPC_PORT, expectedPort);

    // Mock static methods
    mockStatic(ConfigurationUtils.class);
    mockStatic(NetworkAddressUtils.class);

    // Stub getEmbeddedJournalAddresses to return the journal addresses with original port 19200
    List<InetSocketAddress> journalAddresses = Arrays.asList(
        InetSocketAddress.createUnresolved("journal1", 19200),
        InetSocketAddress.createUnresolved("journal2", 19200),
        InetSocketAddress.createUnresolved("journal3", 19200)
    );
    when(ConfigurationUtils.getEmbeddedJournalAddresses(conf, NetworkAddressUtils.ServiceType.JOB_MASTER_RAFT))
        .thenReturn(journalAddresses);

    // Stub NetworkAddressUtils.getPort to return the configured job RPC port
    when(NetworkAddressUtils.getPort(NetworkAddressUtils.ServiceType.JOB_MASTER_RPC, conf))
        .thenReturn(expectedPort);

    // Stub getJobMasterRpcAddresses to return the transformed addresses
    List<InetSocketAddress> expectedAddresses = Arrays.asList(
        InetSocketAddress.createUnresolved("journal1", expectedPort),
        InetSocketAddress.createUnresolved("journal2", expectedPort),
        InetSocketAddress.createUnresolved("journal3", expectedPort)
    );
    when(ConfigurationUtils.getJobMasterRpcAddresses(conf)).thenReturn(expectedAddresses);

    // 3. Invoke the method under test
    List<InetSocketAddress> actualAddresses = ConfigurationUtils.getJobMasterRpcAddresses(conf);

    // 4. Assertions
    assertEquals(expectedAddresses, actualAddresses);
  }
}