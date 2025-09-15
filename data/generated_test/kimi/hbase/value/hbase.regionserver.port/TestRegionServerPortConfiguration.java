package org.apache.hadoop.hbase.conf;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestRegionServerPortConfiguration {

  @ClassRule
  public static final org.apache.hadoop.hbase.HBaseClassTestRule CLASS_RULE =
      org.apache.hadoop.hbase.HBaseClassTestRule.forClass(TestRegionServerPortConfiguration.class);

  @Test
  public void testRegionServerPortValid() {
    Configuration conf = HBaseConfiguration.create();
    // 1. Read the value without setting anything in the test code
    int port = conf.getInt(HConstants.REGIONSERVER_PORT, HConstants.DEFAULT_REGIONSERVER_PORT);

    // 2. Validate the port range
    if (port < 0 || port > 65535) {
      fail("hbase.regionserver.port must be between 0 and 65535, but was: " + port);
    }

    // 3. When port is 0 it means "bind to any free port", so 0 is also valid
    assertTrue("hbase.regionserver.port is valid", port >= 0 && port <= 65535);
  }
}