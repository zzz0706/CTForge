package alluxio.util;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.util.List;

public class ConfigurationUtilsTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testMalformedAddressThrows() {
        // 1. Create a new AlluxioConfiguration instance
        InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

        // 2. Prepare the test conditions: set a malformed address string
        conf.set(PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES, "host1:port1,host2:20002");

        // 3. Test code: expect a NumberFormatException due to malformed port "port1"
        thrown.expect(NumberFormatException.class);
        thrown.expectMessage("For input string: \"port1\"");

        // 4. Invoke the method under test
        List<java.net.InetSocketAddress> result = ConfigurationUtils.getJobMasterEmbeddedJournalAddresses(conf);
    }
}