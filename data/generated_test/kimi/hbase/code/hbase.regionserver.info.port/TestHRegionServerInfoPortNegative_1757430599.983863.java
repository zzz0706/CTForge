package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, SmallTests.class})
public class TestHRegionServerInfoPortNegative {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHRegionServerInfoPortNegative.class);

  private Configuration conf;

  @Before
  public void setUp() {
    // 1. Use the hbase 2.2.2 API correctly to obtain configuration values
    conf = HBaseConfiguration.create();
    conf.setInt(HConstants.REGIONSERVER_INFO_PORT, -1);
    // Prevent Path creation failure by setting hbase.rootdir
    conf.set(HConstants.HBASE_DIR, "file:///tmp/hbase-test");
  }

  @Test
  public void testPortNegativeDisablesInfoServer() throws Exception {
    // 2. Prepare the test conditions
    // Stub subclass to avoid full RegionServer startup
    HRegionServer rs = new HRegionServer(conf) {
      @Override
      protected void handleReportForDutyResponse(
          java.util.Map<byte[], java.util.List<java.lang.String>> result) {
        // no-op
      }

      @Override
      public ServerName getServerName() {
        return ServerName.valueOf("localhost", 12345, System.currentTimeMillis());
      }
    };

    // 3. Test code
    Method putUpWebUIMethod = HRegionServer.class.getDeclaredMethod("putUpWebUI");
    putUpWebUIMethod.setAccessible(true);
    int actualPort = (Integer) putUpWebUIMethod.invoke(rs);

    // 4. Code after testing
    assertEquals(-1, actualPort);
    assertNull(rs.infoServer);
  }
}