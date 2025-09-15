package alluxio.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.PropertyKey;
import alluxio.util.network.NetworkAddressUtils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.InetSocketAddress;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest({NetworkAddressUtils.class})
public class ConfigurationUtilsTest {

    @Test
    public void testFallbackToLocalAddress() {
        // 1. Create a fresh configuration without explicit overrides
        AlluxioConfiguration conf = new InstancedConfiguration(new AlluxioProperties());

        // 2. Ensure neither JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES nor MASTER_EMBEDDED_JOURNAL_ADDRESSES is set
        //    (InstancedConfiguration defaults are empty, so nothing to unset)

        // 3. Mock NetworkAddressUtils.getConnectAddress to return localhost:20003
        mockStatic(NetworkAddressUtils.class);
        InetSocketAddress expectedAddress = new InetSocketAddress("localhost", 20003);
        when(NetworkAddressUtils.getConnectAddress(NetworkAddressUtils.ServiceType.JOB_MASTER_RAFT, conf))
            .thenReturn(expectedAddress);

        // 4. Call the method under test
        List<InetSocketAddress> actualAddresses =
            ConfigurationUtils.getJobMasterEmbeddedJournalAddresses(conf);

        // 5. Assert the result
        assertEquals(1, actualAddresses.size());
        assertEquals(expectedAddress, actualAddresses.get(0));
    }
}