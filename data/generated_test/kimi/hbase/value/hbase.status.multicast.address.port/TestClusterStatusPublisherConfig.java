package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, SmallTests.class})
public class TestClusterStatusPublisherConfig {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestClusterStatusPublisherConfig.class);

  private static Configuration conf;

  @BeforeClass
  public static void setUp() {
    // 1. Obtain configuration values from hbase-default.xml / hbase-site.xml, no hard-coded values.
    conf = new Configuration();
  }

  @Test
  public void testStatusMulticastPort() throws IOException {
    // 2. Prepare: fetch the configured value (or default).
    int port = conf.getInt(HConstants.STATUS_MULTICAST_PORT,
                           HConstants.DEFAULT_STATUS_MULTICAST_PORT);

    // 3. Test: validate port range (0-65535).
    assertTrue("Configured hbase.status.multicast.address.port (" + port +
               ") must be between 0 and 65535 inclusive",
               port >= 0 && port <= 65535);

    // 4. Post-test: nothing to clean up.
  }

  @Test
  public void testStatusMulticastAddressResolvable() throws IOException {
    // 2. Prepare: fetch address and port.
    String mcAddress = conf.get(HConstants.STATUS_MULTICAST_ADDRESS,
                                HConstants.DEFAULT_STATUS_MULTICAST_ADDRESS);
    int port = conf.getInt(HConstants.STATUS_MULTICAST_PORT,
                           HConstants.DEFAULT_STATUS_MULTICAST_PORT);

    // 3. Test: ensure address is resolvable.
    try {
      InetAddress.getByName(mcAddress);
    } catch (UnknownHostException e) {
      throw new IOException("Multicast address " + mcAddress + " is not resolvable", e);
    }

    // 4. Post-test: nothing to clean up.
  }
}