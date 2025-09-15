package alluxio.util;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.conf.Source;
import alluxio.util.ConfigurationUtils;
import alluxio.util.network.NetworkAddressUtils;
import org.junit.Assert;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.HashMap;

public class ConfigurationUtilsTest {

    @Test
    public void testGetEmbeddedJournalAddressesMasterRaft() {
        // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.

        // 2. Prepare the test conditions.
        // Create a mock configuration instance
        AlluxioConfiguration mockConf = PowerMockito.mock(AlluxioConfiguration.class);
        // Mock the retrieval of configuration values for MASTER_EMBEDDED_JOURNAL_ADDRESSES
        PowerMockito.when(mockConf.isSet(PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES)).thenReturn(true);
        PowerMockito.when(mockConf.getList(PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES, ","))
            .thenReturn(Arrays.asList("host1:19998", "host2:19999", "host3:20000"));

        // Create a new configuration map for testing
        HashMap<String, String> configMap = new HashMap<>();
        configMap.put(PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES.getName(), "host1:19998,host2:19999,host3:20000");

        // Update the ServerConfiguration's global configuration using the correct method
        ServerConfiguration.merge(configMap, Source.RUNTIME);

        try {
            // 3. Test code.
            // Call the method under test with the correct arguments
            List<InetSocketAddress> addresses = ConfigurationUtils.getEmbeddedJournalAddresses(
                mockConf, NetworkAddressUtils.ServiceType.MASTER_RAFT);

            // Assert the addresses match the expected test data
            Assert.assertNotNull(addresses);
            Assert.assertEquals(3, addresses.size());
            Assert.assertEquals("host1", addresses.get(0).getHostName());
            Assert.assertEquals(19998, addresses.get(0).getPort());
            Assert.assertEquals("host2", addresses.get(1).getHostName());
            Assert.assertEquals(19999, addresses.get(1).getPort());
            Assert.assertEquals("host3", addresses.get(2).getHostName());
            Assert.assertEquals(20000, addresses.get(2).getPort());
        } finally {
            // 4. Code after testing.
            // Clear the test configuration to restore the default state of ServerConfiguration
            ServerConfiguration.reset();
        }
    }
}