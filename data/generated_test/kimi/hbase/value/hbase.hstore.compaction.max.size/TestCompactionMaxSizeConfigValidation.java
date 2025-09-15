package org.apache.hadoop.hbase.regionserver.compactions;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.hbase.HBaseClassTestRule;

@Category(SmallTests.class)
public class TestCompactionMaxSizeConfigValidation {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCompactionMaxSizeConfigValidation.class);

  private static final String KEY = "hbase.hstore.compaction.max.size";

  @BeforeClass
  public static void setUp() {
    // No configuration values are set within the test code.
  }

  @Test
  public void testMaxSizeIsPositive() {
    Configuration conf = HBaseConfiguration.create();
    conf.addResource("hbase-site.xml");
    long maxSize = conf.getLong(KEY, Long.MAX_VALUE);
    assertTrue("hbase.hstore.compaction.max.size must be positive", maxSize > 0);
  }

  @Test
  public void testMaxSizeIsLong() {
    Configuration conf = HBaseConfiguration.create();
    conf.addResource("hbase-site.xml");
    String[] raw = conf.getStringCollection(KEY).toArray(new String[0]);
    if (raw != null && raw.length > 0) {
      try {
        Long.parseLong(raw[0].trim());
      } catch (NumberFormatException e) {
        assertFalse("hbase.hstore.compaction.max.size must be a valid long integer", true);
      }
    }
  }
}