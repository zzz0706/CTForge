package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Addressing;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, SmallTests.class})
public class TestHRegionServerInfoBindAddressConfig {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHRegionServerInfoBindAddressConfig.class);

  @Test
  public void testBindAddressIsLocal() {
    Configuration conf = new Configuration();
    // Do NOT set hbase.regionserver.info.bindAddress so we read the runtime value
    String bindAddress = conf.get("hbase.regionserver.info.bindAddress", "0.0.0.0");

    try {
      if (!Addressing.isLocalAddress(InetAddress.getByName(bindAddress))) {
        fail("hbase.regionserver.info.bindAddress='" + bindAddress +
             "' does not resolve to a local address. Correct the value.");
      }
    } catch (UnknownHostException e) {
      fail("hbase.regionserver.info.bindAddress='" + bindAddress +
           "' is not a valid hostname or IP address: " + e.getMessage());
    }
  }
}