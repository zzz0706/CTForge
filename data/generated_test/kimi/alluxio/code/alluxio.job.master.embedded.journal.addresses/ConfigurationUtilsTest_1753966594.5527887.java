package alluxio.util;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;

import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ConfigurationUtilsTest {

  @Test
  public void testExplicitJobMasterEmbeddedJournalAddresses() {
    // 1. You need to use the alluxio2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    InstancedConfiguration conf = new InstancedConfiguration(
        alluxio.conf.ServerConfiguration.global().copyProperties());
    conf.set(PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES,
             "host1:20001,host2:20002,host3:20003");
    // Ensure the fallback property is NOT set so only the explicit list is used
    conf.unset(PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES);

    // 3. Test code.
    List<InetSocketAddress> actual =
        ConfigurationUtils.getJobMasterEmbeddedJournalAddresses(conf);

    List<InetSocketAddress> expected = Arrays.asList(
        new InetSocketAddress("host1", 20001),
        new InetSocketAddress("host2", 20002),
        new InetSocketAddress("host3", 20003));

    assertEquals(expected, actual);
    // 4. Code after testing.
  }
}