package alluxio.util;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import alluxio.util.network.NetworkAddressUtils;

import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ConfigurationUtilsTest {

    @Test
    public void testFallbackToMasterAddressesWithPortOverride() {
        // 1. Create a new AlluxioConfiguration instance
        InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

        // 2. Prepare test conditions
        conf.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES, "master1:19200,master2:19200,master3:19200");
        conf.set(PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_PORT, 20003);

        // 3. Test code - call the method under test
        List<InetSocketAddress> result = ConfigurationUtils.getJobMasterEmbeddedJournalAddresses(conf);

        // 4. Code after testing - assertions
        List<InetSocketAddress> expected = Arrays.asList(
            new InetSocketAddress("master1", 20003),
            new InetSocketAddress("master2", 20003),
            new InetSocketAddress("master3", 20003)
        );
        assertEquals(expected, result);
    }
}