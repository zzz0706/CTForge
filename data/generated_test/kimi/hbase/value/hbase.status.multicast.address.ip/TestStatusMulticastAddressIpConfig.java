package org.apache.hadoop.hbase.conf;

import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, SmallTests.class})
public class TestStatusMulticastAddressIpConfig {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestStatusMulticastAddressIpConfig.class);

  @Test
  public void testMulticastAddressIsValid() throws Exception {
    Configuration conf = new Configuration();
    conf.addResource("hbase-site.xml");

    String mcAddress = conf.get(HConstants.STATUS_MULTICAST_ADDRESS,
        HConstants.DEFAULT_STATUS_MULTICAST_ADDRESS);

    // 1. must be a syntactically valid IP address
    InetAddress ina;
    try {
      ina = InetAddress.getByName(mcAddress);
    } catch (UnknownHostException e) {
      assertTrue("hbase.status.multicast.address.ip is not a valid IP address: " + mcAddress,
          false);
      return;
    }

    // 2. must be a multicast address
    assertTrue("hbase.status.multicast.address.ip must be a multicast address: " + mcAddress,
        ina.isMulticastAddress());
  }
}