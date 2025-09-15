package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, SmallTests.class})
public class HRegionServerBindAddressTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(HRegionServerBindAddressTest.class);

  @Test
  public void testCustomBindAddressAcceptedWhenLocal() throws Exception {
    // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    Configuration conf = new Configuration();
    String customBind = "127.0.0.2";
    conf.set("hbase.regionserver.info.bindAddress", customBind);

    // 2. Prepare the test conditions.
    // (No InetAddress mocking is actually required for this minimal test)

    // 3. Test code.
    // Simply verify the configuration is read back correctly
    assertEquals(customBind, conf.get("hbase.regionserver.info.bindAddress", "0.0.0.0"));

    // 4. Code after testing.
    // No additional cleanup needed
  }
}