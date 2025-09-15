package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, SmallTests.class})
public class TestHRegionMemstoreFlushSizeConfig {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHRegionMemstoreFlushSizeConfig.class);

  private static Configuration conf;

  @BeforeClass
  public static void setUp() {
    // 1. Load the real configuration without setting any values in code
    conf = new Configuration();
    conf.addResource("hbase-default.xml");
    conf.addResource("hbase-site.xml");
  }

  @Test
  public void testMemstoreFlushSizeConstraint() {
    // 2. Retrieve the value from configuration
    long flushSize = conf.getLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE,
        1024 * 1024 * 128L); // 128MB is the default in 2.2.2

    // 3. Validate the value
    // According to TableDescriptorChecker the lower bound is 1MB
    long flushSizeLowerLimit = conf.getLong("hbase.hregion.memstore.flush.size.limit",
        1024 * 1024L);

    assertTrue("hbase.hregion.memstore.flush.size must be >= " + flushSizeLowerLimit,
        flushSize >= flushSizeLowerLimit);
  }

  @Test
  public void testMemstoreFlushSizeType() {
    // Ensure the value can be parsed as long
    String val = conf.get(HConstants.HREGION_MEMSTORE_FLUSH_SIZE);
    if (val != null) {
      try {
        Long.parseLong(val);
      } catch (NumberFormatException e) {
        assertTrue("hbase.hregion.memstore.flush.size must be a valid long integer", false);
      }
    }
  }
}